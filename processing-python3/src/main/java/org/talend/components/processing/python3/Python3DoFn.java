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

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.talend.components.processing.python3.luci.LuciDoItDoItInvoker;

import java.io.IOException;

public class Python3DoFn extends DoFn<IndexedRecord, IndexedRecord> {

    private Python3Configuration configuration = null;

    private static final String sessionId = LuciDoItDoItInvoker.createSessionId();

    private transient LuciDoItDoItInvoker invoker = null;

    Python3DoFn withConfiguration(Python3Configuration configuration) {
        this.configuration = configuration;
        return this;
    }

    @Setup
    public void setup() throws Exception {
    }

    @ProcessElement
    public void processElement(ProcessContext context) throws IOException {
        if (invoker == null)
            invoker = LuciDoItDoItInvoker.of(sessionId);
        if (!invoker.isPythonServerUnpacked())
            invoker.unpackPythonServerFiles();
        context.output(context.element());
    }

    @Teardown
    public void tearDown() {
    }
}
