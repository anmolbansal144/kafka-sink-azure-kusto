package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import com.csvreader.CsvWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.OutputStream;
import java.nio.charset.Charset;

public class CsvRecordWriterProvider implements RecordWriterProvider<KustoSinkConfig> {

  @Override
  public String getExtension() {
    return null;
  }

  @Override
  public RecordWriter getRecordWriter(final KustoSinkConfig conf, final String filename, OutputStream out) {
    CsvWriter writer = new CsvWriter(out,',', Charset.defaultCharset());
    return new RecordWriter() {

      /**
       * This method writes record to outputstream.

       * @param record Represents SinkRecord
       */
      @Override
      public void write(SinkRecord record) {
        try {
          writer.writeRecord(record.value().toString().split(","));
        } catch (Exception e) {
          throw new ConnectException("Writing records failed",e);
        }
      }

      @Override
      public void commit() {
        writer.flush();
      }

      @Override
      public void close() {
        if (writer != null) {
          writer.close();
        }
      }
    };
  }
}



