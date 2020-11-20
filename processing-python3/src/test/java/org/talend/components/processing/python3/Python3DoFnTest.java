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
package org.talend.components.processing.python3;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.DoFnTester;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.talend.daikon.avro.GenericDataRecordHelper;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;

public class Python3DoFnTest {

    private static IndexedRecord inputIndexedRecord = null;

    private static IndexedRecord outputIndexedRecord = null;

    private static String utf8Sample = "Les naïfs ægithales hâtifs pondant à Noël où il gèle sont sûrs d'être "
            + "déçus en voyant leurs drôles d'œufs abîmés.";

    @BeforeAll
    public static void setUp() throws IOException {
        Object[] inputAsObject1 = new Object[] { "rootdata",
                new Object[] { "subdata", new Object[] { "subsubdata1", 28, 42l }, "subdata2" } };
        Schema inputSchema = GenericDataRecordHelper.createSchemaFromObject("MyRecord", inputAsObject1);
        inputIndexedRecord = GenericDataRecordHelper.createRecord(inputAsObject1);
    }

    @Test
    public void testBasic() throws Exception {
        Python3Configuration configuration = new Python3Configuration();
        configuration.setPythonCode("output = input");
        Python3DoFn function = new Python3DoFn();
        function.withConfiguration(configuration);
        function.withConfiguration(configuration);
        DoFnTester<IndexedRecord, IndexedRecord> fnTester = DoFnTester.of(function);
        List<IndexedRecord> outputs = fnTester.processBundle(inputIndexedRecord);
        assertThat(outputs, hasSize(1));
    }
}
