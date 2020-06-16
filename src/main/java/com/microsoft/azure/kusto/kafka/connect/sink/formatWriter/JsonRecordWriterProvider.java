package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;
/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;


import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;

public class JsonRecordWriterProvider implements RecordWriterProvider<KustoSinkConfig> {

    private static final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
    private static final String LINE_SEPARATOR = System.lineSeparator();
    private static final byte[] LINE_SEPARATOR_BYTES
            = LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8);
    private final ObjectMapper mapper = new ObjectMapper();
    private final JsonConverter converter;
    OutputStream out = null;

    public JsonRecordWriterProvider(JsonConverter converter) {
        this.converter = converter;
    }

    @Override
    public String getExtension() {
        return null;
    }

    public RecordWriter getRecordWriter(KustoSinkConfig conf, String filename, OutputStream out) {
        this.out = out;
        return getRecordWriter(conf, filename);
    }

    @Override
    public RecordWriter getRecordWriter(KustoSinkConfig conf, final String filename) {
        try {
            return new RecordWriter() {
                final JsonGenerator writer = mapper.getFactory()
                .createGenerator(out)
                .setRootValueSeparator(null);

        @Override
        public void write(SinkRecord record) {
            log.trace("Sink record: {}", record);
            try {
                Object value = record.value();
                if (value instanceof Struct) {
                    byte[] rawJson = converter.fromConnectData(
                            record.topic(),
                            record.valueSchema(),
                            value
                    );
                } else {
                    writer.writeObject(value);
                    writer.writeRaw(LINE_SEPARATOR);
                }
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        }

        @Override
        public void commit() {
            try {
                // Flush is required here, because closing the writer will close the underlying S3
                // output stream before committing any data to S3.
                writer.flush();
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        }

        @Override
        public void close() {
            try {
                writer.close();
            } catch (IOException e) {
                throw new ConnectException(e);
            }
        }
    };
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}


