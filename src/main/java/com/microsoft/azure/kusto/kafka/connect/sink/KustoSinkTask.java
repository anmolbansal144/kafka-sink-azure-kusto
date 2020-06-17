package com.microsoft.azure.kusto.kafka.connect.sink;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.ingest.IngestClient;
import com.microsoft.azure.kusto.ingest.IngestionMapping;
import com.microsoft.azure.kusto.ingest.IngestionProperties;
import com.microsoft.azure.kusto.ingest.IngestClientFactory;
import com.microsoft.azure.kusto.ingest.source.CompressionType;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.NotFoundException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.*;

/**
 * Kusto sink uses file system to buffer records.
 * Every time a file is rolled, we used the kusto client to ingest it.
 * Currently only ingested files are "commited" in the sense that we can advance the offset according to it.
 */
public class KustoSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(KustoSinkTask.class);
    private final Set<TopicPartition> assignment;
    private Map<String, TopicIngestionProperties> topicsToIngestionProps;
    IngestClient kustoIngestClient;
    Map<TopicPartition, TopicPartitionWriter> writers;
    private long maxFileSize;
    private long flushInterval;
    private String tempDir;
    private KustoSinkConfig config;
    private final Time time = new SystemTime();

    public KustoSinkTask() {
        assignment = new HashSet<>();
        writers = new HashMap<>();
    }

    public static IngestClient createKustoIngestClient(KustoSinkConfig config) throws Exception {
        if (config.getKustoAuthAppid() != null) {
            if (config.getKustoAuthAppkey() == null) {
                throw new ConfigException("Kusto authentication missing App Key.");
            }

            ConnectionStringBuilder kcsb = ConnectionStringBuilder.createWithAadApplicationCredentials(
                config.getKustoUrl(),
                config.getKustoAuthAppid(),
                config.getKustoAuthAppkey(),
                config.getKustoAuthAuthority()
            );
            kcsb.setClientVersionForTracing(Version.CLIENT_NAME + ":" + Version.getVersion());

            return IngestClientFactory.createClient(kcsb);
        }

        if (config.getKustoAuthUsername() != null) {
            if (config.getKustoAuthPassword() == null) {
                throw new ConfigException("Kusto authentication missing Password.");
            }

            return IngestClientFactory.createClient(ConnectionStringBuilder.createWithAadUserCredentials(
                config.getKustoUrl(),
                config.getKustoAuthUsername(),
                config.getKustoAuthPassword()
            ));
        }
        throw new ConfigException("Kusto authentication method must be provided.");
    }

    public static Map<String, TopicIngestionProperties> getTopicsToIngestionProps(KustoSinkConfig config) throws ConfigException {
        Map<String, TopicIngestionProperties> result = new HashMap<>();

        try {
            if (config.getKustoTopicToTableMapping() != null) {
                JSONArray mappings = new JSONArray(config.getKustoTopicToTableMapping());

                for (int i =0;i< mappings.length();i++) {

                    JSONObject mapping = mappings.getJSONObject(i);

                    try {
                        String db = mapping.getString("db");
                        String table = mapping.getString("table");

                        String format = mapping.optString("format");
                        CompressionType compressionType = StringUtils.isBlank(mapping.optString("eventDataCompression")) ? null : CompressionType.valueOf(mapping.optString("eventDataCompression"));

                        IngestionProperties props = new IngestionProperties(db, table);

                        if (format != null && !format.isEmpty()) {
                            if (format.equals("json") || format.equals("singlejson")){
                                props.setDataFormat("multijson");
                            }
                            props.setDataFormat(format);
                        }

                        String mappingRef = mapping.optString("mapping");
                        log.info("db{},table{},format{},mapping{}",db,table,format,mappingRef);
                        if (mappingRef != null && !mappingRef.isEmpty()) {
                            if (format != null) {
                                if (format.equals(IngestionProperties.DATA_FORMAT.json.toString())){
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.json);
                                } else if (format.equals(IngestionProperties.DATA_FORMAT.avro.toString())){
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.avro);
                                } else if (format.equals(IngestionProperties.DATA_FORMAT.parquet.toString())) {
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.parquet);
                                } else if (format.equals(IngestionProperties.DATA_FORMAT.orc.toString())){
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.orc);
                                } else {
                                    props.setIngestionMapping(mappingRef, IngestionMapping.IngestionMappingKind.csv);
                                }
                            }
                        }
                        TopicIngestionProperties topicIngestionProperties = new TopicIngestionProperties();
                        topicIngestionProperties.eventDataCompression = compressionType;
                        topicIngestionProperties.ingestionProperties = props;
                        result.put(mapping.getString("topic"), topicIngestionProperties);
                    } catch (Exception ex) {
                        log.error("TOPICMAPPING Validation",ex);
                        throw new ConfigException("Malformed topics to kusto ingestion props mappings", ex);
                    }
                }

                return result;
            }
        }
        catch (Exception ex) {
            throw new ConfigException(String.format("Error trying to parse kusto ingestion props %s",ex.getMessage()));
        }

        throw new ConfigException("Malformed topics to kusto ingestion props mappings");
    }

    public TopicIngestionProperties getIngestionProps(String topic) {
        return topicsToIngestionProps.get(topic);
    }

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void open(Collection<TopicPartition> partitions) throws ConnectException {
        assignment.addAll(partitions);
        for (TopicPartition tp : assignment) {
            TopicIngestionProperties ingestionProps = getIngestionProps(tp.topic());
            log.debug(String.format("Open Kusto topic: '%s' with partition: '%s'", tp.topic(), tp.partition()));
            if (ingestionProps == null) {
                throw new ConnectException(String.format("Kusto Sink has no ingestion props mapped for the topic: %s. please check your configuration.", tp.topic()));
            } else {
                TopicPartitionWriter writer = new TopicPartitionWriter(tp, kustoIngestClient, ingestionProps, tempDir, maxFileSize, flushInterval,config);
                writer.open();
                writers.put(tp, writer);
            }
        }
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        for (TopicPartition tp : partitions) {
            final long maxAttempts = config.getMaxRetry() + 1;
            int attempts = 1;
            boolean indexed = false;
            while (!indexed) {
                try {
                    writers.get(tp).close();
                    writers.remove(tp);
                    assignment.remove(tp);
                    indexed = true;
                } catch (ConnectException e) {
                    if (attempts < maxAttempts) {
                        long sleepTimeMs = RetryUtil.computeRandomRetryWaitTimeInMillis(attempts - 1,
                            config.getRetryBaxkOff());
                        log.warn("Error closing writer for {} with attempt {}/{}, "
                                + "will attempt retry after {} ms. Failure reason: {}",
                            tp,attempts, maxAttempts, sleepTimeMs, e.getMessage());
                        time.sleep(sleepTimeMs);
                    } else {
                        config.handleErrors(String.format("Error closing writer for %s", tp), e);
                    }
                    attempts++;
                }
            }
        }
    }

    @Override
    public void start(Map<String, String> props) throws ConnectException {
        try {
            config = new KustoSinkConfig(props);
            String url = config.getKustoUrl();

            topicsToIngestionProps = getTopicsToIngestionProps(config);
            // this should be read properly from settings
            kustoIngestClient = createKustoIngestClient(config);
            tempDir = config.getKustoSinkTempDir();
            maxFileSize = config.getKustoFlushSize();
            flushInterval = config.getKustoFlushIntervalMS();
            log.info(String.format("Kafka Kusto Sink started. target cluster: (%s), source topics: (%s)", url, topicsToIngestionProps.keySet().toString()));
            open(context.assignment());

        } catch (ConfigException ex) {
            throw new ConnectException(String.format("Kusto Connector failed to start due to configuration error.", ex));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void stop() throws ConnectException {
        log.warn("Stopping KustoSinkTask");
        for (TopicPartitionWriter writer : writers.values()) {
            writer.close();
        }
        final long maxAttempts = config.getMaxRetry() + 1;
        int attempts = 1;
        boolean indexed = false;
        while (!indexed) {
            try {

                kustoIngestClient.close();
                indexed = true;
            } catch (IOException e) {
                if (attempts < maxAttempts) {
                    long sleepTimeMs = RetryUtil.computeRandomRetryWaitTimeInMillis(attempts - 1,
                        config.getRetryBaxkOff());
                    log.warn("Error closing kusto client with attempt {}/{}, "
                            + "will attempt retry after {} ms. Failure reason: {}",
                        attempts, maxAttempts, sleepTimeMs, e.getMessage());
                    time.sleep(sleepTimeMs);
                } else {
                    config.handleErrors("Error closing kusto client", e);
                }
                attempts++;
            }
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) throws ConnectException {
        for (SinkRecord record : records) {
            log.debug("record to topic:" + record.topic());

            TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
            TopicPartitionWriter writer = writers.get(tp);

            if (writer == null) {
                NotFoundException e = new NotFoundException(String.format("Received a record without a mapped writer for topic:partition(%s:%d), dropping record.", tp.topic(), tp.partition()));
                config.handleErrors(e.getMessage(), e);
            }

            writer.writeRecord(record);
        }
    }

    // This is a neat trick, since our rolling files commit whenever they like, offsets may drift
    // from what kafka expects. So basically this is to re-sync topic-partition offsets with our sink.
    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> offsets
    ) {
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
        for (TopicPartition tp : assignment) {

            Long offset = writers.get(tp).lastCommittedOffset;

            if (offset != null) {
                log.debug("Forwarding to framework request to commit offset: {} for {}", offset, tp);
                offsetsToCommit.put(tp, new OffsetAndMetadata(offset));
            }
        }

        return offsetsToCommit;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) throws ConnectException {
        // do nothing , rolling files can handle writing

    }
}
