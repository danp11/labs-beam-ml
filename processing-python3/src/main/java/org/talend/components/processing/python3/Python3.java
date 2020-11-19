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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.MigrationHandler;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;

import javax.json.JsonObject;

import java.util.Map;

@Version(value = 1, migrationHandler = Python3.Migration.class)
@Processor(name = "Python3")
@Icon(value = Icon.IconType.CUSTOM, custom = "python")
@Documentation("This component executes python code on incoming data.")
public class Python3 extends PTransform<PCollection<IndexedRecord>, PCollection<IndexedRecord>> {

    private Python3Configuration configuration;

    public Python3(@Option("configuration") final Python3Configuration configuration) {
        this.configuration = configuration;
    }

    @ElementListener
    public void onElement(final JsonObject element, @Output final OutputEmitter<JsonObject> output) {
        // Ignored
    }

    @Override
    public PCollection expand(PCollection<IndexedRecord> in) {
        Python3DoFn doFn = new Python3DoFn().withConfiguration(configuration);
        return in.apply("Python3", ParDo.of(doFn));
    }

    public static class Migration implements MigrationHandler {

        private static final Logger log = LoggerFactory.getLogger(Migration.class);

        @Override
        public Map<String, String> migrate(final int incomingVersion, final Map<String, String> incomingData) {
            log.debug("Starting Python3 component migration");
            return incomingData;
        }
    }
}
