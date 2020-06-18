package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class StringRecordWriterProvider implements RecordWriterProvider<KustoSinkConfig> {
  private static final Logger log = LoggerFactory.getLogger(StringRecordWriterProvider.class);
  @Override
  public RecordWriter getRecordWriter(KustoSinkConfig conf, String filename, OutputStream out) {
    return new RecordWriter() {
      long size =0;

      @Override
      public void write(SinkRecord record) throws IOException {
        byte[] value = null;
        value = String.format("%s\n", record.value()).getBytes(StandardCharsets.UTF_8);
        out.write(value);
        size += value.length;
      }

      @Override
      public void close() {
        try {
          out.close();
        } catch (IOException e) {
          throw new DataException(e);
        }
      }

      @Override
      public void commit() {
        try {
          out.flush();
        } catch (IOException e) {
          throw new DataException(e);
        }
      }

      @Override
      public long getDataSize() {
        return size;
      }
    };
  }

}
