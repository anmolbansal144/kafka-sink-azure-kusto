package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import io.confluent.kafka.serializers.NonRecordContainer;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Schema;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig;

import java.io.IOException;
import java.io.OutputStream;

public class AvroRecordWriterProvider implements RecordWriterProvider<KustoSinkConfig> {
  private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
  private final AvroData avroData = new AvroData(50);
  OutputStream out = null;
  @Override
  public String getExtension() {
    return null;
  }

  public RecordWriter getRecordWriter(KustoSinkConfig conf, String filename, OutputStream out) {
    this.out = out;
    return getRecordWriter(conf, filename);
  }

  @Override
  public RecordWriter getRecordWriter(KustoSinkConfig conf, String filename) {
    return new RecordWriter() {
      final DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>());
      Schema schema;

      @Override
      public void write(SinkRecord record) {
        if (schema == null) {
          schema = record.valueSchema();
          try {
            log.info("Opening record writer for: {}", filename);
            org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
            writer.setCodec(CodecFactory.fromString(conf.getAvroCodec()));
            writer.create(avroSchema, out);
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        log.trace("Sink record: {}", record);
        Object value = avroData.fromConnectData(schema, record.value());
        try {
          // AvroData wraps primitive types so their schema can be included. We need to unwrap
          // NonRecordContainers to just their value to properly handle these types
          if (value instanceof NonRecordContainer) {
            writer.append(((NonRecordContainer) value).getValue());
          } else {
            writer.append(value);
          }
        } catch (IOException e) {
          throw new DataException(e);
        }
      }

      @Override
      public void close() {
        try {
          writer.close();
        } catch (IOException e) {
          throw new DataException(e);
        }
      }

      @Override
      public void commit() {
        try {
          writer.flush();
        } catch (IOException e) {
          throw new DataException(e);
        }
      }
    };
  }
}
