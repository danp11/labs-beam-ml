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

import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import lucitesttest.All;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * This unit test verifies that the protocol.avsc is consistent with the test IDL.
 *
 * <p>
 * IDL is a much easier to read format than JSON schemas, but Python does not compile IDL like
 * Java yet.
 *
 * <p>
 * To change the protocol via the IDL file:
 *
 * <ol>
 * <li>Update the IDL file with the new messages, and ensure that it is present in the All record.
 * <li>Run this test. It should fail, but overwrite the known schema in the python project.
 * <li>Rebuild the entire project. It should regenerate the Avro classes from the python project.
 * </ol>
 */
class LuciProtocolTest {

    /** This is the source of truth for the generated code. */
    private static final Path PROTOCOL_AVSC = Paths.get("lucidoitdoit", "lucidoitdoit", "protocol.avsc");

    /** Follow up from the current working directory until the PROTOCOL_AVSC can be found. */
    private Path findProtocol() {
        Path p = Paths.get(".").toAbsolutePath();
        while (!p.getRoot().equals(p)) {
            Path protocol = p.resolve(PROTOCOL_AVSC);
            if (Files.exists(protocol))
                return protocol;
            p = p.getParent();
        }
        return null;
    }

    @Test
    public void testProtocol(@TempDir Path tmpDir) throws Exception {
        Path protocol = findProtocol();
        assertThat(protocol, notNullValue());

        String avscSchema = Files.lines(protocol, StandardCharsets.UTF_8).reduce((s, s2) -> s + "\n" + s2).orElse(null);

        String idlSchema = All.getClassSchema().toString(true);
        idlSchema = idlSchema.replaceAll("lucitesttest", "lucidoitdoit");
        try (PrintWriter out = new PrintWriter(new FileOutputStream(protocol.toFile()))) {
            out.println(idlSchema);
        }

        assertThat(idlSchema, is(avscSchema));
    }
}
