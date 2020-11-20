/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
 *
 * This source code is available under agreement available at
 * %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
 *
 * You should have received a copy of the agreement
 * along with this program; if not, write to Talend SA
 * 9 rue Pages 92150 Suresnes, France
 */
package org.talend.components.processing.python3.luci;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.processing.python3.Python3;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/** Sets up a connection to a Python3 server. */
public class LuciDoItDoItInvoker implements Closeable {

    public static final Path DEFAULT_ROOT_STORAGE_PATH = Paths.get("/tmp", "luci");

    public static final Path INSTALL_WHL_NAME = Paths.get("cache", "lucidoitdoit-0.1-py3-none-any.whl");

    public static final Path INSTALL_SETUP_NAME = Paths.get("cache", "lucisetup");

    public static final Path SESSION_SOCKET_NAME = Paths.get("lucidoitdoit.socket");

    public static final Path SESSION_PID_NAME = Paths.get("lucidoitdoit.pid");

    private static final Logger log = LoggerFactory.getLogger(Python3.Migration.class);

    /** Used to construct a session ID. */
    private static final char[] RND = "abcdefghijklmnopqrstvwxyz0123456789".toCharArray();

    /** The default length for a session ID. */
    private static final int SESSION_ID_LENGTH = 16;

    /**
     * A session ID is used to run a unique server on this machine. This can be used, along with the
     * rootStorageDir to determine whether the server is already running, which port it is serving on,
     * and its environment.
     */
    private final String sessionId;

    /** On-disk storage location for all generated files. */
    private final Path rootStorageDir;

    /** On-disk location for the server wheel to be installed. */
    private final Path installWhl;

    /** On-disk location for the server setup script. */
    private final Path installSetup;

    /**
     * On-disk path for the current session. This should contain the entire environment for the
     * Python3 server.
     */
    private final Path sessionEnvironment;

    private Process luciProcess = null;

    private Integer port = null;

    private Socket socket = null;

    private LuciDoItDoItInvoker(String sessionId, Path rootStorageDir, Path installWhl, Path installSetup) {
        this.sessionId = sessionId;
        this.rootStorageDir = rootStorageDir.toAbsolutePath();
        this.sessionEnvironment = this.rootStorageDir.resolve(sessionId);
        this.installWhl = installWhl.toAbsolutePath();
        this.installSetup = installSetup.toAbsolutePath();
    }

    public static LuciDoItDoItInvoker of(String sessionId) {
        return of(DEFAULT_ROOT_STORAGE_PATH, sessionId);
    }

    public static LuciDoItDoItInvoker of(Path rootStorageDir, String sessionId) {
        return of(sessionId, rootStorageDir, null, null);
    }

    public static LuciDoItDoItInvoker of(String sessionId, Path rootStorageDir, String whlName, String setupName) {
        rootStorageDir = rootStorageDir != null ? rootStorageDir : DEFAULT_ROOT_STORAGE_PATH;
        return new LuciDoItDoItInvoker(sessionId, rootStorageDir,
                rootStorageDir.resolve(whlName != null ? Paths.get(whlName) : INSTALL_WHL_NAME),
                rootStorageDir.resolve(whlName != null ? Paths.get(setupName) : INSTALL_SETUP_NAME));
    }

    /** @return a session ID for the PythonServerInvoker. */
    public static String createSessionId() {
        StringBuilder uid = new StringBuilder("luci");
        Random rnd = new Random();
        for (int i = 0; i < SESSION_ID_LENGTH; i++)
            uid.append(RND[rnd.nextInt(RND.length)]);
        return uid.toString();
    }

    /**
     * @return true if the python server files are available on the local filesystem and can be used
     * to start a server.
     */
    public boolean isPythonServerUnpacked() {
        return Files.exists(installWhl) && Files.exists(installSetup);
    }

    /**
     * Unpack the two files necessary to launch the server. The Python wheel should be available as a
     * resource on the classpath and it contains the setup script that should be able to launch the
     * entire environment.
     *
     * @throws IOException if the files could not be unpacked.
     */
    public void unpackPythonServerFiles() throws IOException {

        // Get the wheel as a resource on the classpath.
        try (InputStream src = LuciDoItDoItInvoker.class.getClassLoader()
                .getResourceAsStream(installWhl.getFileName().toString())) {
            if (src == null) {
                throw new IOException("Bad setup, missing " + installWhl.getFileName().toString());
            }

            byte[] buf = new byte[8192];
            Files.createDirectories(installWhl.getParent());
            Files.createDirectories(installSetup.getParent());

            // Copy the wheel to its final destination.
            try (FileOutputStream dst = new FileOutputStream(installWhl.toFile())) {
                int length;
                while ((length = src.read(buf)) > 0) {
                    dst.write(buf, 0, length);
                }
            }

            // Read inside the wheel to find the setup script.
            try (FileInputStream whl = new FileInputStream(installWhl.toFile());
                    ZipInputStream zis = new ZipInputStream(new BufferedInputStream(whl))) {
                ZipEntry ze;
                while ((ze = zis.getNextEntry()) != null) {
                    if (installSetup.getFileName().equals(Paths.get(ze.getName()).getFileName())) {
                        try (FileOutputStream fos = new FileOutputStream(installSetup.toFile());
                                BufferedOutputStream bos = new BufferedOutputStream(fos, buf.length)) {
                            int length;
                            while ((length = zis.read(buf)) > 0) {
                                bos.write(buf, 0, length);
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * Clean up the resources from unpacking the python server files on the filesystem.
     *
     * @throws IOException if the files could not be removed.
     */
    public boolean cleanPythonServerFiles() throws IOException {
        return cleanFile(installWhl) | cleanFile(installSetup);
    }

    public boolean isServerStarted() {

        // Return fast if the process has never been started.
        if (luciProcess == null)
            return false;

        // Check that all of the files exist.
        boolean sessionEnvironmentExists = Files.exists(sessionEnvironment);
        boolean sessionSocketFileExists = Files.exists(sessionEnvironment.resolve(SESSION_SOCKET_NAME));
        boolean sessionPidFileExists = Files.exists(sessionEnvironment.resolve(SESSION_PID_NAME));

        log.debug("Session environment path: {} ({})", sessionEnvironment.toString(), sessionEnvironmentExists);
        log.debug("Session socket file     : {} ({})", sessionEnvironment.toString(), sessionSocketFileExists);
        log.debug("Session pid file        : {} ({})", sessionEnvironment.toString(), sessionPidFileExists);
        if (!sessionEnvironmentExists || !sessionSocketFileExists || !sessionPidFileExists)
            return false;

        if (luciProcess.isAlive()) {
            // Check that the pid is alive, and that the socket is serving.
        }

        return false;
    }

    public void startServer() throws IOException, InterruptedException {
        // If the process is running, then skip.
        if (isServerStarted())
            return;

        // If the files already exist, then skip this step.
        if (!isPythonServerUnpacked())
            unpackPythonServerFiles();

        ProcessBuilder pb = new ProcessBuilder(Arrays.asList(installSetup.toString(), sessionId));

        HashMap<String, String> env = new HashMap<>();
        env.put("LUCIDOITDOIT_WHL", installWhl.toString());
        pb.environment().putAll(env);

        // TODO: Set up the process as you want, logs, etc.
        pb.inheritIO();
        // pb.redirectErrorStream(true);
        // pb.redirectOutput(outputFile);

        log.debug("Attempting to start process with command: {}", pb.command());
        luciProcess = pb.start();

        // Wait for this file to exist before continuing.
        String contents = readFromFile(sessionEnvironment.resolve(SESSION_SOCKET_NAME), 10, 1000);
        port = Integer.valueOf(contents.trim());
    }

    public void shutdownServer() throws IOException, InterruptedException {
        // If the process is running, then skip.
        if (luciProcess != null && luciProcess.isAlive())
            return;

        // If the files already exist, then skip this step.
        if (!Files.exists(installWhl) || !Files.exists(installSetup))
            unpackPythonServerFiles();

        ProcessBuilder pb = new ProcessBuilder(Arrays.asList(installSetup.toString(), sessionId));

        HashMap<String, String> env = new HashMap<>();
        env.put("LUCIDOITDOIT_WHL", installWhl.toString());
        pb.environment().putAll(env);

        // TODO: Set up the process as you want, logs, etc.
        pb.inheritIO();
        // pb.redirectErrorStream(true);
        // pb.redirectOutput(outputFile);

        log.debug("Attempting to start process with command: {}", pb.command());
        luciProcess = pb.start();

        // Wait for this file to exist before continuing.
        String contents = readFromFile(sessionEnvironment.resolve(SESSION_SOCKET_NAME), 10, 1000);
        port = Integer.valueOf(contents.trim());
    }

    public Socket getSocket() throws IOException {
        if (socket == null)
            socket = new Socket((String) null, port);
        return socket;
    }

    /**
     * Shutdown
     *
     * @throws IOException
     */
    @Override
    public void close() throws IOException {
        // If the socket is open, close it.
        // If the server process is running, stop it. Clean it up.
        // Check for other dead servers?
        // Clean up the environment?
    }

    /**
     * Attempts to read a file as a String, blocking until the file exists.
     *
     * @param file The file to read.
     * @param attempts The number of attempts to make while reading the file.
     * @param timeoutMs The timeout between attempts.
     * @return The contents of the file as a String, or null if the file could not be read or doesn't
     * exist before timeout.
     */
    private String readFromFile(Path file, int attempts, final long timeoutMs) throws IOException, InterruptedException {
        while (attempts-- > 0) {
            if (Files.exists(file)) {
                return Files.lines(file, StandardCharsets.UTF_8).reduce((s, s2) -> s + "\n" + s2).orElse(null);
            } else {
                if (!luciProcess.isAlive()) {
                    throw new IllegalStateException(
                            "The Python3 server died before a connection could be made." + luciProcess.exitValue());
                }
                Thread.sleep(timeoutMs);
            }
        }
        // On timeout.
        return null;
    }

    /**
     * Attempts to delete a file and all of its empty parent directories.
     *
     * <p>
     * This will prune until we reach the parent root storage directory for all files, until a
     * non-empty directory is found.
     *
     * @param file The file to remove.
     * @return Whether or not any file was removed.
     */
    private boolean cleanFile(Path file) throws IOException {
        boolean anyFileDeleted = false;
        try {
            while (file.startsWith(rootStorageDir)) {
                anyFileDeleted |= Files.deleteIfExists(file);
                file = file.getParent();
            }
        } catch (DirectoryNotEmptyException ignored) {
        }
        return anyFileDeleted;
    }

    public Short getPort() {
        return port == null ? null : port.shortValue();
    }
}
