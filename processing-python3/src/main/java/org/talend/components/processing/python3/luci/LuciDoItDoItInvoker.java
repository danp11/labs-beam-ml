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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
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

    public static final Path DEFAULT_LUCI_PATH = Paths.get("/tmp", "luci");

    /** Relative to the root storage path, where shared files are stored. */
    public static final Path GLOBAL_FILES = Paths.get("global", "files");

    public static final String INSTALL_WHL_NAME = "lucidoitdoit-0.1-py3-none-any.whl";

    public static final String INSTALL_SETUP_NAME = "lucisetup";

    public static final String SESSION_SOCKET_NAME = "lucidoitdoit.socket";

    public static final String SESSION_PID_NAME = "lucidoitdoit.pid";

    private static final Logger log = LoggerFactory.getLogger(LuciDoItDoItInvoker.class);

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

    /** On-disk storage location for all generated files for the python server. */
    private final Path luciDir;

    /** On-disk storage location for shared files. */
    private final Path globalFileDir;

    /** On-disk storage location for the session. */
    private final Path sessionDir;

    /** On-disk location for the server setup script. */
    private final Path installSetup;

    private Process luciProcess = null;

    private Integer port = null;

    private Socket socket = null;

    public LuciDoItDoItInvoker(String sessionId, Path rootStorageDir) {
        this.sessionId = sessionId;
        this.luciDir = (rootStorageDir != null ? rootStorageDir : DEFAULT_LUCI_PATH).toAbsolutePath();
        this.sessionDir = this.luciDir.resolve(sessionId);
        this.globalFileDir = this.luciDir.resolve(GLOBAL_FILES);
        this.installSetup = this.globalFileDir.resolve(INSTALL_SETUP_NAME).toAbsolutePath();
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
        // This is the last step of unpacking the server files, so if it exists, the filesystem
        // must be unpacked.
        return Files.exists(installSetup);
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
        try (InputStream src = LuciDoItDoItInvoker.class.getClassLoader().getResourceAsStream(INSTALL_WHL_NAME)) {
            if (src == null) {
                throw new IOException("Bad setup, missing " + INSTALL_WHL_NAME);
            }

            byte[] buf = new byte[8192];
            Files.createDirectories(globalFileDir);

            File tmpWhl = File.createTempFile("luci.wheel", "tmp");
            File tmpSetup = File.createTempFile("luci.setup", "tmp");

            // Extract the wheel to a temporary destination.
            try (FileOutputStream dst = new FileOutputStream(tmpWhl)) {
                int length;
                while ((length = src.read(buf)) > 0) {
                    dst.write(buf, 0, length);
                }
            }

            // Read inside the wheel to find the setup script.
            try (FileInputStream whl = new FileInputStream(tmpWhl);
                    ZipInputStream zis = new ZipInputStream(new BufferedInputStream(whl))) {
                ZipEntry ze;
                while ((ze = zis.getNextEntry()) != null) {
                    if (installSetup.getFileName().equals(Paths.get(ze.getName()).getFileName())) {
                        try (FileOutputStream fos = new FileOutputStream(tmpSetup);
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

            // Move both files to their final locations
            try {
                Files.move(tmpWhl.toPath(), globalFileDir.resolve(INSTALL_WHL_NAME));
            } catch (FileAlreadyExistsException ignored) {
                // If it already exists, then another thread or process installed it in the meantime.
                Files.delete(tmpWhl.toPath());
            }
            try {
                Files.move(tmpSetup.toPath(), installSetup);
            } catch (FileAlreadyExistsException ignored) {
                // If it already exists, then another thread or process installed it in the meantime.
                Files.delete(tmpWhl.toPath());
            }
        }
    }

    public boolean isServerStarted() {

        // Return fast if the process has never been started.
        if (luciProcess == null)
            return false;

        // Check that all of the files exist.
        boolean sessionDirExists = Files.exists(sessionDir);
        boolean sessionSocketFileExists = Files.exists(sessionDir.resolve(SESSION_SOCKET_NAME));
        boolean sessionPidFileExists = Files.exists(sessionDir.resolve(SESSION_PID_NAME));

        log.debug("Session environment path: {} ({})", sessionDir.toString(), sessionDir);
        log.debug("Session socket file     : {} ({})", sessionDir.resolve(SESSION_SOCKET_NAME).toString(),
                sessionSocketFileExists);
        log.debug("Session pid file        : {} ({})", sessionDir.resolve(SESSION_PID_NAME), sessionPidFileExists);
        if (!sessionDirExists || !sessionSocketFileExists || !sessionPidFileExists)
            return false;

        // Try to connect to

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
        env.put("LUCIDOITDOIT_WHL", globalFileDir.resolve(INSTALL_WHL_NAME).toString());
        env.put("LUCIDOITDOIT_ENV", globalFileDir.resolve("env").toString());
        pb.environment().putAll(env);

        // TODO: Set up the process as you want, logs, etc.
        pb.inheritIO();
        // pb.redirectErrorStream(true);
        // pb.redirectOutput(outputFile);

        log.debug("Attempting to start process with command: {}", pb.command());
        luciProcess = pb.start();

        // Wait for this file to exist before continuing.
        String contents = readFromFile(sessionDir.resolve(SESSION_SOCKET_NAME), 10, 1000);
        port = Integer.valueOf(contents.trim());
    }

    public void shutdownServer() throws IOException, InterruptedException {
        // If the process is running, then skip.
        if (luciProcess != null && luciProcess.isAlive())
            return;

        // If the files already exist, then skip this step.
        if (!Files.exists(globalFileDir.resolve(INSTALL_WHL_NAME)) || !Files.exists(installSetup))
            unpackPythonServerFiles();

        ProcessBuilder pb = new ProcessBuilder(Arrays.asList(installSetup.toString(), sessionId));

        HashMap<String, String> env = new HashMap<>();
        env.put("LUCIDOITDOIT_WHL", globalFileDir.resolve(INSTALL_WHL_NAME).toString());
        pb.environment().putAll(env);

        // TODO: Set up the process as you want, logs, etc.
        pb.inheritIO();
        // pb.redirectErrorStream(true);
        // pb.redirectOutput(outputFile);

        log.debug("Attempting to start process with command: {}", pb.command());
        luciProcess = pb.start();

        // Wait for this file to exist before continuing.
        String contents = readFromFile(sessionDir.resolve(SESSION_SOCKET_NAME), 10, 1000);
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
            while (file.startsWith(globalFileDir)) {
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
