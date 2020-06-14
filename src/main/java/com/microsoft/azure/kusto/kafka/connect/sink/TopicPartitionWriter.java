package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import com.microsoft.azure.kusto.ingest.source.FileSourceInfo;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONArray;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

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
    private final Time time = new SystemTime();
    Producer<byte[], byte[]> producer;

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
        //FileSourceInfo fileSourceInfo = new FileSourceInfo(fileDescriptor.path, fileDescriptor.rawBytes);
        System.out.println("--------------------------------xxxxxxxxx----------------------------------------------");
        final long maxAttempts = kustoSinkConfig.getMaxRetry() + 1;
        int attempts = 1;
        boolean indexed = false;
        long x=0;
        try {
            Reader fileReader = new FileReader(fileDescriptor.path);

            int data = fileReader.read();
            while (data != -1) {
                data = fileReader.read();
                x += data;
            }
            fileReader.close();
        } catch (Exception e) {
            System.out.println("error in reader");
        }
        FileSourceInfo fileSourceInfo = new FileSourceInfo(fileDescriptor.path,x);
        while (!indexed) {
            try {
                client.ingestFromFile(fileSourceInfo, ingestionProps);
                System.out.println("rawBytesIngestion" + fileSourceInfo.getRawSizeInBytes());
                System.out.println("filePath" + fileDescriptor.path);
                System.out.println("Ingestion done");
                log.info(String.format("Kusto ingestion: file (%s) of size (%s) at current offset (%s)", fileDescriptor.path, fileDescriptor.rawBytes, currentOffset));
                this.lastCommittedOffset = currentOffset;
                indexed = true;
            } catch (Exception e) {
                if (attempts < maxAttempts) {
                    long sleepTimeMs = RetryUtil.computeRandomRetryWaitTimeInMillis(attempts - 1,
                        kustoSinkConfig.getRetryBaxkOff());
                    log.warn("Ingestion Failed for file: {}, size: {} with attempt {}/{}, "
                            + "will attempt retry after {} ms. Failure reason: {}",
                        fileDescriptor.file.getName(), attempts, maxAttempts, sleepTimeMs, e.getMessage());
                    time.sleep(sleepTimeMs);
                } else {
                    List<byte[]> records = fileDescriptor.records;
                    for (byte[] record : records) {
                        System.out.println("error :" + record);
                        sendCorruptedRecords(record);
                    }
                    kustoSinkConfig.handleErrors("Ingestion Failed for file :" + fileDescriptor.file.getName(), e);
                }
                attempts++;

            }
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
        if(ingestionProps.getDataFormat().equals(IngestionMapping.IngestionMappingKind.parquet.toString())) {
            System.out.println("hello : " + record);
            if (record == null) {
                this.currentOffset = record.kafkaOffset();
            } else {
                try {
                this.currentOffset = record.kafkaOffset();
                fileWriter.writeParquet(record);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else if(ingestionProps.getDataFormat().equals(IngestionMapping.IngestionMappingKind.avro.toString())) {
            System.out.println("hello : " + record);
            if (record == null) {
                this.currentOffset = record.kafkaOffset();
            } else {
                try {
                    this.currentOffset = record.kafkaOffset();
                    fileWriter.writeParquet(record);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        else {

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
                byte[] valueBytes = (byte[]) record.value();
                byte[] separator = "\n".getBytes(StandardCharsets.UTF_8);
                byte[] valueWithSeparator = new byte[valueBytes.length + separator.length];

                System.arraycopy(valueBytes, 0, valueWithSeparator, 0, valueBytes.length);
                System.arraycopy(separator, 0, valueWithSeparator, valueBytes.length, separator.length);
                value = valueWithSeparator;
                sendCorruptedRecords(value);
            }

            if (value == null) {
                this.currentOffset = record.kafkaOffset();
            } else {
                final long maxAttempts = kustoSinkConfig.getMaxRetry() + 1;
                int attempts = 1;
                boolean indexed = false;
                while (!indexed) {
                    try {
                        this.currentOffset = record.kafkaOffset();
                        fileWriter.write(value);
                        System.out.println("records" + record.value() + " : " + value);
                        indexed = true;
                    } catch (IOException e) {
                        if (attempts < maxAttempts) {
                            long sleepTimeMs = RetryUtil.computeRandomRetryWaitTimeInMillis(attempts - 1,
                                kustoSinkConfig.getRetryBaxkOff());
                            log.warn("File write failed with attempt {}/{}, "
                                    + "will attempt retry after {} ms. Failure reason: {}",
                                attempts, maxAttempts, sleepTimeMs, e.getMessage());
                            time.sleep(sleepTimeMs);
                        } else {
                            sendCorruptedRecords(value);
                            kustoSinkConfig.handleErrors("File write failed", e);
                        }
                        attempts++;
                    }
                }
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
                //!shouldCompressData ? 0 : flushInterval,
                flushInterval,
                //shouldCompressData,
            !shouldCompressData,
            kustoSinkConfig);
        producer = createProducer();

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

    public Producer<byte[], byte[]> createProducer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", kustoSinkConfig.getKustoReporterBootStrapServer());
        properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        Producer<byte[], byte[]> producer = new KafkaProducer(properties);
        return producer;
    }

    public void sendCorruptedRecords(byte[] value) {
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(kustoSinkConfig.getKustoReporterErrorTopic(), value);
        try {
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    System.out.println("exception");
                    kustoSinkConfig.handleErrors(String.format("Failed to log missing record in topic %s",kustoSinkConfig.getKustoReporterErrorTopic()), exception);
                }
                System.out.println("record success");
            });
        } catch (IllegalStateException e) {
            /*
             * This might occur when connector task has been closed,
             * which means the datagram-socket as well as missing-record-producer
             * has been closed already.
             *
             * The poll() thread might still try to push already enqueued records to missing-record-topic
             * and the missing-record-producer will end up throwing an IllegalStateException.
             *
             * We handle this by logging the missing records to Connect logs.
             * */
            log.error("Missing Record Kafka Producer has already been closed.");
        }

    }
}
