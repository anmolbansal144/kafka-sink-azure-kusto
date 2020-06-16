package com.microsoft.azure.kusto.kafka.connect.sink.format;

public interface RecordWriterProvider<C> {
  String getExtension();

  RecordWriter getRecordWriter(C conf, String fileName);
}
