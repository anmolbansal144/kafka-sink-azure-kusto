package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

public class TopicPartitionWriter {

    private static final Logger log = LoggerFactory.getLogger(KustoSinkTask.class);
    private CompressionType eventDataCompression;
    private TopicPartition tp;
    private IngestClient client;
    private IngestionProperties ingestionProps;
    private String basePath;
    private long flushInterval;
    private long fileThreshold;
    private KustoSinkConfig kustoSinkConfig;

    FileWriter fileWriter;
    long currentOffset;
    Long lastCommittedOffset;
    public TopicPartitionWriter(TopicPartition tp, IngestClient client, TopicIngestionProperties ingestionProps, String basePath, long fileThreshold, long flushInterval, KustoSinkConfig config) {
        this.tp = tp;
        this.client = client;
        this.ingestionProps = ingestionProps.ingestionProperties;
        this.fileThreshold = fileThreshold;
        this.basePath = basePath;
        this.flushInterval = flushInterval;
        this.currentOffset = 0;
        this.eventDataCompression = ingestionProps.eventDataCompression;
        kustoSinkConfig = config;
    }

    public void handleRollFile(SourceFile fileDescriptor) {
        FileSourceInfo fileSourceInfo = new FileSourceInfo(fileDescriptor.path, fileDescriptor.rawBytes);

        try {
            client.ingestFromFile(fileSourceInfo, ingestionProps);

            log.info(String.format("Kusto ingestion: file (%s) of size (%s) at current offset (%s)", fileDescriptor.path, fileDescriptor.rawBytes, currentOffset));
            this.lastCommittedOffset = currentOffset;
        } catch (Exception e) {
            kustoSinkConfig.handleErrors("Ingestion Failed for file :" + fileDescriptor.file.getName(),e);
        }
    }

    public String getFilePath() {
        long nextOffset = fileWriter != null && fileWriter.isDirty() ? currentOffset + 1 : currentOffset;

        String compressionExtension = "";
        if (shouldCompressData(ingestionProps, null) || eventDataCompression != null) {
            if(eventDataCompression != null) {
                compressionExtension = "." + eventDataCompression.toString();
            } else {
                compressionExtension = ".gz";
            }
        }

        return Paths.get(basePath, String.format("kafka_%s_%s_%d.%s%s", tp.topic(), tp.partition(), nextOffset, ingestionProps.getDataFormat(), compressionExtension)).toString();
    }

    public void writeRecord(SinkRecord record) {
        byte[] value = null;

        // TODO: should probably refactor this code out into a value transformer
        if (record.valueSchema() == null || record.valueSchema().type() == Schema.Type.STRING) {
            value = String.format("%s\n", record.value()).getBytes(StandardCharsets.UTF_8);
        } else if (record.valueSchema().type() == Schema.Type.BYTES) {
            byte[] valueBytes = (byte[]) record.value();
            byte[] separator = "\n".getBytes(StandardCharsets.UTF_8);
            byte[] valueWithSeparator = new byte[valueBytes.length + separator.length];

            System.arraycopy(valueBytes, 0, valueWithSeparator, 0, valueBytes.length);
            System.arraycopy(separator, 0, valueWithSeparator, valueBytes.length, separator.length);

            value = valueWithSeparator;
        } else {
            kustoSinkConfig.handleErrors(String.format("Unexpected value type, skipping record %s", record),null);
        }

        if (value == null) {
            this.currentOffset = record.kafkaOffset();
        } else {
            try {
                this.currentOffset = record.kafkaOffset();
                fileWriter.write(value);
            } catch (IOException e) {
                kustoSinkConfig.handleErrors("File write failed", e);
            }
        }
    }

    public void open() {
        // Should compress binary files
        boolean shouldCompressData = shouldCompressData(this.ingestionProps, this.eventDataCompression);

        fileWriter = new FileWriter(
                basePath,
                fileThreshold,
                this::handleRollFile,
                this::getFilePath,
                !shouldCompressData ? 0 : flushInterval,
                shouldCompressData,
            kustoSinkConfig);
    }

    public void close() {
        try {
            fileWriter.rollback();
            // fileWriter.close(); TODO ?
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static boolean shouldCompressData(IngestionProperties ingestionProps, CompressionType eventDataCompression) {
        return !(ingestionProps.getDataFormat().equals(IngestionProperties.DATA_FORMAT.avro.toString())
                || ingestionProps.getDataFormat().equals(IngestionProperties.DATA_FORMAT.parquet.toString())
                || ingestionProps.getDataFormat().equals(IngestionProperties.DATA_FORMAT.orc.toString())
                || eventDataCompression != null);
    }
}
