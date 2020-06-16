package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.connect.storage.hive.HiveSchemaConverter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

//import org.apache.orc.Writer;
//import org.apache.orc.OrcFile;
;

public class OrcRecordWriterProvider implements RecordWriterProvider<KustoSinkConfig> {
    private static final Logger log = LoggerFactory.getLogger(OrcRecordWriterProvider.class);
    private static final String EXTENSION = ".orc";

    @Override
    public String getExtension() {
        return null;
    }

    @Override
    public RecordWriter getRecordWriter(KustoSinkConfig conf, String filename) {
        Path path = new Path(filename);

        return new RecordWriter() {
            Writer writer;
            TypeInfo typeInfo;
            Schema schema;

            @Override
            public void write(SinkRecord record) {
                if (schema == null) {
                    schema = (Schema) record.valueSchema();
                    if (schema.type() == Schema.Type.STRUCT) {

                        OrcFile.WriterCallback writerCallback = new OrcFile.WriterCallback() {
                            @Override
                            public void preStripeWrite(OrcFile.WriterContext writerContext) {
                            }

                            @Override
                            public void preFooterWrite(OrcFile.WriterContext writerContext) {
                            }
                        };

                        typeInfo = HiveSchemaConverter.convert((org.apache.kafka.connect.data.Schema) schema);
                        ObjectInspector objectInspector = OrcStruct.createObjectInspector(typeInfo);

                        log.info("Opening ORC record writer for: {}", filename);
                        try {
                            writer = OrcFile.createWriter(path, OrcFile.writerOptions(conf.getHadoopConfiguration())
                                            .inspector(objectInspector)
                                            .callback(writerCallback));
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }

                if (schema.type() == Schema.Type.STRUCT) {
                    log.trace(
                            "Writing record from topic {} partition {} offset {}",
                            record.topic(),
                            record.kafkaPartition(),
                            record.kafkaOffset()
                    );

                    Struct struct = (Struct) record.value();
                    OrcStruct row = OrcUtil.createOrcStruct(typeInfo, OrcUtil.convertStruct(struct));
                    try {
                        writer.addRow(row);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                } else {
                    throw new ConnectException(
                            "Top level type must be STRUCT but was " + schema.type().getName()
                    );
                }
            }

            @Override
            public void close() {
                try {
                    if (writer != null) {
                        writer.close();
                    }
                } catch (IOException e) {
                    throw new ConnectException("Failed to close ORC writer:", e);
                }
            }

            @Override
            public void commit() { }
        };
    }
}

