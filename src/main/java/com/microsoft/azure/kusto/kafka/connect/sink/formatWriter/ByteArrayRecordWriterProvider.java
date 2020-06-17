package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

import org.apache.kafka.connect.converters.ByteArrayConverter;

public class ByteArrayRecordWriterProvider implements RecordWriterProvider<KustoSinkConfig> {

  private static final Logger log = LoggerFactory.getLogger(ByteArrayRecordWriterProvider.class);
  private final ByteArrayConverter converter = new ByteArrayConverter();
  private final byte[] lineSeparatorBytes = "\n".getBytes(StandardCharsets.UTF_8);
  OutputStream out = null;

  @Override
  public String getExtension() {
    return null;
  }

  @Override
  public RecordWriter getRecordWriter(final KustoSinkConfig conf, final String filename, OutputStream out) {
    return new RecordWriter() {

      @Override
      public void write(SinkRecord record) {
        log.trace("Sink record: {}", record);
        try {
          byte[] bytes = converter.fromConnectData(
              record.topic(), record.valueSchema(), record.value());
          out.write(bytes);
          out.write(lineSeparatorBytes);
        } catch (IOException | DataException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void commit() {
        try {
          out.flush();
        } catch (IOException e) {
          throw new ConnectException(e);
        }
      }

      @Override
      public void close() {}
    };
  }
}
