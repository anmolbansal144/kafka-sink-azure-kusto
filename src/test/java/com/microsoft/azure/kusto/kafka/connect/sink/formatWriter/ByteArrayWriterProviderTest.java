package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ByteArrayWriterProviderTest {

  @Test
  public void testByteData() throws IOException {
    List<SinkRecord> records = new ArrayList<SinkRecord>();
    for(int i=0;i<10;i++) {
      records.add(new SinkRecord("mytopic", 0, null, null, Schema.BYTES_SCHEMA, String.format("hello-%s",i).getBytes(), i));
    }
    File file = new File("abc.bin");
    KustoSinkConfig config = new KustoSinkConfig(getProperties());
    ByteRecordWriterProvider writer = new ByteRecordWriterProvider();
    FileOutputStream fos = new FileOutputStream(file);
    OutputStream out=fos;
    RecordWriter rd = writer.getRecordWriter(config,file.getPath(),out);
    for(SinkRecord record : records){
      rd.write(record);
    }
    rd.commit();
    BufferedReader br = new BufferedReader(new FileReader(file));
    String st;
    int i=0;
    while ((st = br.readLine()) != null) {
      assertEquals(st, String.format("hello-%s", i));
      i++;
    }
    assertEquals(rd.getDataSize(),80);
    file.delete();
  }

  protected Map<String, String> getProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("kusto.url", "xxx");
    props.put("kusto.tables.topics.mapping", "[{'topic': 'xxx','db': 'xxx', 'table': 'xxx','format': 'avro', 'mapping':'avri'}]");
    return props;
  }
}
