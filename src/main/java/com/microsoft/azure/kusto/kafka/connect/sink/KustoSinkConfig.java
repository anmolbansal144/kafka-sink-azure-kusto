package com.microsoft.azure.kusto.kafka.connect.sink;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.TimeUnit;



public class KustoSinkConfig extends AbstractConfig {

    private static final Logger log = LoggerFactory.getLogger(KustoSinkConfig.class);
    /**
     * FAIL: stop connector in case of any Error while processing records
     * IGNORE: Ignore and continue in case of any Error while processing records
     * LOG: Log and Ignore in case of any Error while processing records
     */
    public enum ErrorBehaviour {

        FAIL("Stops the connector when an error occurs "
            + "while processiong records.\n"),

        IGNORE("Continues to process next set of records "
            + "when error occurs while processiong records.\n"),

        LOG("Logs the error message when error occurs "
            + "while processiong records and "
            + "continues to process next set of records, "
            + "available in Connect logs.");

        private String desc;

        ErrorBehaviour(String desc) {
            this.desc = desc;
        }

        public String getDesc() {
            return desc;
        }
    }
    // TODO: this might need to be per kusto cluster...
    static final String KUSTO_URL = "kusto.url";
    static final String KUSTO_TABLES_MAPPING = "kusto.tables.topics_mapping";

    static final String KUSTO_AUTH_USERNAME = "kusto.auth.username";
    static final String KUSTO_AUTH_PASSWORD = "kusto.auth.password";

    static final String KUSTO_AUTH_APPID = "kusto.auth.appid";
    static final String KUSTO_AUTH_APPKEY = "kusto.auth.appkey";
    static final String KUSTO_AUTH_AUTHORITY = "kusto.auth.authority";

    static final String KUSTO_SINK_TEMPDIR = "kusto.sink.tempdir";
    static final String KUSTO_SINK_FLUSH_SIZE = "kusto.sink.flush_size";
    static final String KUSTO_SINK_FLUSH_INTERVAL_MS = "kusto.sink.flush_interval_ms";
    static final String KUSTO_SINK_WRITE_TO_FILES = "kusto.sink.write_to_files";

    public static final String BEHAVIOR_ON_ERROR_CONFIG = "behavior.on.error";
    public static final String BEHAVIOR_ON_ERROR_DEFAULT = ErrorBehaviour.FAIL.name().toLowerCase();
    private static final String BEHAVIOR_ON_ERROR_DOC = "Error handling behavior setting for "
        + "storage connectors. "
        + "Must be configured to one of the following:\n"
        + "``fail``\n" + ErrorBehaviour.FAIL.getDesc()
        + "``ignore``\n" + ErrorBehaviour.IGNORE.getDesc()
        + "``log``\n" + ErrorBehaviour.LOG.getDesc();
    private static final String BEHAVIOR_ON_ERROR_DISPLAY = "Behavior On Errors";

    public KustoSinkConfig(ConfigDef config, Map<String, String> parsedConfig) {
        super(config, parsedConfig);
    }

    public KustoSinkConfig(Map<String, String> parsedConfig) {
        this(getConfig(), parsedConfig);
    }

    public static ConfigDef getConfig() {
        return new ConfigDef()
                .define(KUSTO_URL, Type.STRING, Importance.HIGH, "Kusto cluster url")
                .define(KUSTO_TABLES_MAPPING, Type.STRING, null, Importance.HIGH, "Kusto target tables mapping (per topic mapping, 'topic1:table1;topic2:table2;')")
                .define(KUSTO_AUTH_USERNAME, Type.STRING, null, Importance.HIGH, "Kusto auth using username,password combo: username")
                .define(KUSTO_AUTH_PASSWORD, Type.STRING, null, Importance.HIGH, "Kusto auth using username,password combo: password")
                .define(KUSTO_AUTH_APPID, Type.STRING, null, Importance.HIGH, "Kusto auth using appid,appkey combo: app id")
                .define(KUSTO_AUTH_APPKEY, Type.STRING, null, Importance.HIGH, "Kusto auth using appid,appkey combo:  app key")
                .define(KUSTO_AUTH_AUTHORITY, Type.STRING, null, Importance.HIGH, "Kusto auth using appid,appkey combo: authority")
                .define(KUSTO_SINK_TEMPDIR, Type.STRING, System.getProperty("java.io.tempdir"), Importance.LOW, "Temp dir that will be used by kusto sink to buffer records. defaults to system temp dir")
                .define(KUSTO_SINK_FLUSH_SIZE, Type.LONG, FileUtils.ONE_MB, Importance.HIGH, "Kusto sink max buffer size (per topic+partition combo)")
                .define(KUSTO_SINK_FLUSH_INTERVAL_MS, Type.LONG, TimeUnit.MINUTES.toMillis(5), Importance.HIGH, "Kusto sink max staleness in milliseconds (per topic+partition combo)")
                .define(BEHAVIOR_ON_ERROR_CONFIG, ConfigDef.Type.STRING, BEHAVIOR_ON_ERROR_DEFAULT, ConfigDef.Importance.MEDIUM, BEHAVIOR_ON_ERROR_DOC,"Error behaviour",1,ConfigDef.Width.LONG, BEHAVIOR_ON_ERROR_DISPLAY);
    }

    public String getKustoUrl() {
        return this.getString(KUSTO_URL);
    }

    public String getKustoTopicToTableMapping() {
        return this.getString(KUSTO_TABLES_MAPPING);
    }

    public String getKustoAuthUsername() {
        return this.getString(KUSTO_AUTH_USERNAME);
    }

    public String getKustoAuthPassword() {
        return this.getString(KUSTO_AUTH_PASSWORD);
    }

    public String getKustoAuthAppid() {
        return this.getString(KUSTO_AUTH_APPID);
    }

    public String getKustoAuthAppkey() {
        return this.getString(KUSTO_AUTH_APPKEY);
    }

    public String getKustoAuthAuthority() {
        return this.getString(KUSTO_AUTH_AUTHORITY);
    }

    public long getKustoFlushSize() {
        return this.getLong(KUSTO_SINK_FLUSH_SIZE);
    }

    public String getKustoSinkTempDir() {
        return this.getString(KUSTO_SINK_TEMPDIR);
    }

    public long getKustoFlushIntervalMS() {
        return this.getLong(KUSTO_SINK_FLUSH_INTERVAL_MS);
    }

    /**
     * handle error based on configured behavior on error.
     */
    public void handleErrors(String message, Exception ex) {
        String behavior = getString(BEHAVIOR_ON_ERROR_CONFIG);
        if (behavior.equalsIgnoreCase(ErrorBehaviour.FAIL.name())) {
            throw new ConnectException(message,ex);
        } else if (behavior.equalsIgnoreCase(ErrorBehaviour.LOG.name())) {
            log.warn(message, ex);
        } else {
            log.trace(message, ex);
        }
    }

}

