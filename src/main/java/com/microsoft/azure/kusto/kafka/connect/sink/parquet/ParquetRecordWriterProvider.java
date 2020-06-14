package com.microsoft.azure.kusto.kafka.connect.sink.parquet;


import io.confluent.connect.avro.AvroData;
import io.confluent.connect.storage.format.RecordWriter;
import io.confluent.connect.storage.format.RecordWriterProvider;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.parquet.avro.AvroParquetWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class ParquetRecordWriterProvider implements RecordWriterProvider<KustoSinkConfig> {

  private static final Logger log = LoggerFactory.getLogger(ParquetRecordWriterProvider.class);
  private final AvroData avroData = new AvroData(50);

  @Override
  public RecordWriter getRecordWriter(final KustoSinkConfig conf, final String filename) {
    return new RecordWriter() {
      Schema schema = null;
      final CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
      final int blockSize = 256 * 1024 * 1024;
      final int pageSize = 64 * 1024;
      Path path = new Path(filename);
      ParquetWriter<GenericRecord> writer;

      @Override
      public void write(SinkRecord record) {
        if (schema == null) {
          schema = record.valueSchema();
          // may still be null at this point
        }

        if (writer == null) {
          try {
            log.info("Opening record writer for: {}", filename);
            org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
            writer = AvroParquetWriter
                .<GenericRecord>builder(path)
                .withSchema(avroSchema)
                .withCompressionCodec(compressionCodecName)
                .withRowGroupSize(blockSize)
                .withPageSize(pageSize)
                .withDictionaryEncoding(true)
                .withConf(conf.getHadoopConfiguration())
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .build();
            log.debug("Opened record writer for: {}", filename);
          } catch (IOException e) {
            // Ultimately caught and logged in TopicPartitionWriter,
            // but log in debug to provide more context
            log.warn(
                "Error creating {} for file '{}', {}, and schema {}: ",
                AvroParquetWriter.class.getSimpleName(),
                filename,
                compressionCodecName,
                schema,
                e
            );
            throw new ConnectException(e);
          }
        }
        log.trace("Sink record: {}", record.toString());
        Object value = avroData.fromConnectData(schema, record.value());
        try {
          writer.write((GenericRecord) value);

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

      @Override
      public void commit() {
      }
    };
  }

  @Override
  public String getExtension() {
    return null;
  }

}
