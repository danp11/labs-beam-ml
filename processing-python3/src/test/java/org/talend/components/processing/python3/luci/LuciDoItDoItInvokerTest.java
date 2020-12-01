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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LuciDoItDoItInvokerTest {

    @Test
    public void testSetupAndCleanup(@TempDir Path tmpDir) throws Exception {

        // This directory exists before starting.
        assertTrue(Files.exists(tmpDir), "Temp dir: " + tmpDir);

        // This is going to be the file storage location
        Path root = tmpDir.resolve("luci");
        assertFalse(Files.exists(root), "Root dir: " + root);

        LuciDoItDoItInvoker luci = new LuciDoItDoItInvoker("session123", root);

        assertFalse(luci.isPythonServerUnpacked(), "Python server unpacked");
        assertFalse(luci.isServerStarted(), "Server started");

        // Unpack the python files.
        luci.unpackPythonServerFiles();

        // The files should exist.
        Path rootFiles = root.resolve("global").resolve("files");
        assertTrue(Files.exists(root), "Root dir: " + root);
        assertTrue(Files.exists(rootFiles.resolve(LuciDoItDoItInvoker.INSTALL_WHL_NAME)));
        Path script = rootFiles.resolve(LuciDoItDoItInvoker.INSTALL_SETUP_NAME);
        assertTrue(Files.exists(script));
        assertThat(Files.lines(script, StandardCharsets.UTF_8).findFirst(), is(Optional.of("#!/usr/bin/env bash")));

        // // Clean out the python files.
        // assertTrue(luci.cleanPythonServerFiles());
        //
        // // The files are deleted.
        // assertFalse(Files.exists(tmpDir.resolve(LuciDoItDoItInvoker.INSTALL_WHL_NAME)));
        // assertFalse(Files.exists(tmpDir.resolve(LuciDoItDoItInvoker.INSTALL_SETUP_NAME)));
        // // And even the root directory is cleaned, but its parent will not be touched
        // assertFalse(Files.exists(root), "Root dir: " + root);
        // assertTrue(Files.exists(tmpDir), "Temp dir: " + tmpDir);
    }
}
