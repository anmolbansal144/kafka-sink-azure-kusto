package com.microsoft.azure.kusto.kafka.connect.sink.formatWriter;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriter;
import com.microsoft.azure.kusto.kafka.connect.sink.format.RecordWriterProvider;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class JsonRecordWriterProvider implements RecordWriterProvider<KustoSinkConfig> {
  private static final Logger log = LoggerFactory.getLogger(JsonRecordWriterProvider.class);
  private static final String LINE_SEPARATOR = System.lineSeparator();
  private static final byte[] LINE_SEPARATOR_BYTES
      = LINE_SEPARATOR.getBytes(StandardCharsets.UTF_8);

  private final ObjectMapper mapper =  new ObjectMapper();
  private final JsonConverter converter = new JsonConverter();

  public JsonRecordWriterProvider() {
    Map<String, Object> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    converterConfig.put("schemas.cache.size", "50");
    this.converter.configure(converterConfig, false);
  }

  @Override
  public RecordWriter getRecordWriter(final KustoSinkConfig conf, final String filename, OutputStream out) {
    try {
      return new RecordWriter() {
        final JsonGenerator writer = mapper.getFactory()
            .createGenerator(out)
            .setRootValueSeparator(null);
        long size =0;
        @Override
        public void write(SinkRecord record) {
          log.trace("Sink record: {}", record);
          try {
            Object value = record.value();
            writer.writeObject(value);
            writer.writeRaw(LINE_SEPARATOR);
            size+= (value.toString().getBytes().length + LINE_SEPARATOR.getBytes().length);
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        @Override
        public void commit() {
          try {
            writer.flush();
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }

        @Override
        public long getDataSize() {
          return size;
        }

        @Override
        public void close() {
          try {
            writer.close();
            out.close();
          } catch (IOException e) {
            throw new ConnectException(e);
          }
        }
      };
    } catch (IOException e) {
      throw new ConnectException(e);
    }
  }
}