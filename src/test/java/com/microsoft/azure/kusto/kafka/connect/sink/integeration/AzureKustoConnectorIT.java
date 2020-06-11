package com.microsoft.azure.kusto.kafka.connect.sink.integeration;

import com.microsoft.azure.kusto.data.Client;
import com.microsoft.azure.kusto.data.ClientFactory;
import com.microsoft.azure.kusto.data.ConnectionStringBuilder;
import com.microsoft.azure.kusto.data.Results;
import com.microsoft.azure.kusto.data.exceptions.DataClientException;
import com.microsoft.azure.kusto.data.exceptions.DataServiceException;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConfig;

import static com.microsoft.azure.kusto.kafka.connect.sink.integeration.BaseConnectorIT.CONSUME_MAX_DURATION_MS;
import static org.junit.Assert.assertEquals;

public class AzureKustoConnectorIT extends BaseConnectorIT {
  private static final Logger log = LoggerFactory.getLogger(AzureKustoConnectorIT.class);

  private static final List<String> KAFKA_TOPICS = Arrays.asList("kafka1");
  private static final int NUM_OF_PARTITION = 5;
  private static final int NUM_RECORDS_PRODUCED_PER_PARTITION = 100;
  private static final String CONNECTOR_NAME = "azure-kusto-connector";
  private String appId = System.getProperty("appId","");
  private String appKey = System.getProperty("appKey","");
  private String authority = System.getProperty("authority","");
  private String cluster = System.getProperty("cluster","");
  private String database = System.getProperty("database","");
  private KustoSinkConfig config;
  Client engineClient;
  String table;

  @Before
  public void setup() throws Exception {
    startConnect();
    config = new KustoSinkConfig(getProperties());
    ConnectionStringBuilder engineCsb = ConnectionStringBuilder.createWithAadApplicationCredentials(String.format("https://%s.kusto.windows.net", cluster), appId, appKey, authority);
    engineClient = ClientFactory.createClient(engineCsb);
  }

  @After
  public void close() {
    stopConnect();
  }

  @Test
  public void testWithCsvData() throws DataClientException, DataServiceException, IOException, InterruptedException {
    table = "CsvTable";
    connect.kafka().createTopic(KAFKA_TOPICS.get(0), NUM_OF_PARTITION);
    produceCsvRecords();
      engineClient.execute(database, String.format(".create table %s (ColA:string,ColB:int)", table));
      engineClient.execute(database, String.format(".create table ['%s'] ingestion csv mapping 'mappy' " +
          "'[" +
          "{\"column\":\"ColA\", \"DataType\":\"string\", \"Properties\":{\"transform\":\"SourceLocation\"}}," +
          "{\"column\":\"ColB\", \"DataType\":\"int\", \"Properties\":{\"Ordinal\":\"1\"}}," +
          "]'", table));
      Map<String, String> props = getProperties();
    props.put("kusto.sink.tempdir","/home/hasher/microsoft/kafka-sink-azure-kusto/src/test/resources/testE2E/csv");
      props.put("kusto.tables.topics_mapping", "[{'topic': 'kafka1','db': 'anmol', 'table': '" + table + "','format': 'csv', 'mapping':'mappy'}]");
      // start a sink connector
      connect.configureConnector(CONNECTOR_NAME, props);
      // wait for tasks to spin up
      waitForConnectorToStart(CONNECTOR_NAME, 1);
      log.error("Waiting for records in destination topic ...");
      validateExpectedResults(NUM_RECORDS_PRODUCED_PER_PARTITION * NUM_OF_PARTITION);
      engineClient.execute(database, ".drop table " + table);

  }

  @Test
  public void testWithJsonData() throws Exception {
    table = "JsonTable";
    connect.kafka().createTopic(KAFKA_TOPICS.get(0), NUM_OF_PARTITION);
    produceJsonRecords();
    engineClient.execute(database, String.format(".create table %s (TimeStamp: datetime, Name: string, Metric: int, Source:string)",table));
    engineClient.execute(database, String.format(".create table %s ingestion json mapping 'jsonMapping' '[{\"column\":\"TimeStamp\",\"path\":\"$.timeStamp\",\"datatype\":\"datetime\"},{\"column\":\"Name\",\"path\":\"$.name\",\"datatype\":\"string\"},{\"column\":\"Metric\",\"path\":\"$.metric\",\"datatype\":\"int\"},{\"column\":\"Source\",\"path\":\"$.source\",\"datatype\":\"string\"}]'",table));

    Map<String, String> props = getProperties();
    props.put("kusto.sink.tempdir","/home/hasher/microsoft/kafka-sink-azure-kusto/src/test/resources/testE2E/json");
    props.put("kusto.tables.topics_mapping","[{'topic': 'kafka1','db': 'anmol', 'table': '"+ table +"','format': 'json', 'mapping':'jsonMapping'}]");
    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, 1);
    log.error("Waiting for records in destination topic ...");
    validateExpectedResults(NUM_RECORDS_PRODUCED_PER_PARTITION * NUM_OF_PARTITION);
    engineClient.execute(database, ".drop table " + table);
  }

  @Test
  public void testDequeue() throws Exception {
    table = "errorTable";
    String dequeTopic = "errorTopic";
    engineClient.execute(database, String.format(".create table %s (TimeStamp: datetime, Name: string, Metric: int, Source:string)",table));
    engineClient.execute(database, String.format(".create table %s ingestion json mapping 'jsonMapping' '[{\"column\":\"TimeStamp\",\"path\":\"$.timeStamp\",\"datatype\":\"datetime\"},{\"column\":\"Name\",\"path\":\"$.name\",\"datatype\":\"string\"},{\"column\":\"Metric\",\"path\":\"$.metric\",\"datatype\":\"int\"},{\"column\":\"Source\",\"path\":\"$.source\",\"datatype\":\"string\"}]'",table));
    connect.kafka().createTopic(KAFKA_TOPICS.get(0), NUM_OF_PARTITION);
    connect.kafka().createTopic(dequeTopic, NUM_OF_PARTITION);
    produceJsonRecords();
    Map<String, String> props = getProperties();
    props.put("kusto.sink.tempdir","/home/hasher/microsoft/kafka-sink-azure-kusto/src/test/resources/testE2E/json");
    props.put("kusto.tables.topics_mapping","[{'topic': 'kafka1','db': 'anmol', 'table': '"+ table +"','format': 'json', 'mapping':'jsonMapping'}]");
    props.put("kusto.url","xxx");
    props.put("reporter.bootstrap.servers", connect.kafka().bootstrapServers());
    props.put("max.retries","0");

    // start a sink connector
    connect.configureConnector(CONNECTOR_NAME, props);
    // wait for tasks to spin up
    waitForConnectorToStart(CONNECTOR_NAME, 1);
    Thread.sleep(60 * 6 * 1000);
    ConsumerRecords<byte[], byte[]> records = connect.kafka().consume(
        NUM_RECORDS_PRODUCED_PER_PARTITION * NUM_OF_PARTITION,
        CONSUME_MAX_DURATION_MS, dequeTopic
    );
    assertEquals(NUM_RECORDS_PRODUCED_PER_PARTITION * NUM_OF_PARTITION, records.count());
  }

  private void produceCsvRecords(){
    for(int i = 0; i<NUM_OF_PARTITION;i++){
      for (int j = 0; j < NUM_RECORDS_PRODUCED_PER_PARTITION; j++) {
        String kafkaTopic = KAFKA_TOPICS.get(j % KAFKA_TOPICS.size());
        log.debug("Sending message {} with topic {} to Kafka broker {}", kafkaTopic);
        connect.kafka().produce(kafkaTopic,i, null,String.format("stringy message,%s",i));
      }
    }
  }

  private void produceJsonRecords(){
    for(int i = 0; i<NUM_OF_PARTITION;i++){
      for (int j = 0; j < NUM_RECORDS_PRODUCED_PER_PARTITION; j++) {
        String kafkaTopic = KAFKA_TOPICS.get(j % KAFKA_TOPICS.size());
        log.debug("Sending message {} with topic {} to Kafka broker {}", kafkaTopic);
        connect.kafka().produce(kafkaTopic,i, null,String.format("{\"timestamp\" : \"2017-07-23 13:10:11\",\"name\" : \"Anmol\",\"metric\" : \"%s\",\"source\" : \"Demo\"}",i));
      }
    }
  }

  private void validateExpectedResults(Integer expectedNumberOfRows) throws InterruptedException, DataClientException, DataServiceException {
    String query = String.format("%s | count", table);

    Results res = engineClient.execute(database, query);
    Integer timeoutMs = 60 * 8 * 1000;
    Integer rowCount = 0;
    Integer timeElapsedMs = 0;
    Integer sleepPeriodMs = 5 * 1000;

    while (rowCount < expectedNumberOfRows && timeElapsedMs < timeoutMs) {
      Thread.sleep(sleepPeriodMs);
      res = engineClient.execute(database, query);
      System.out.println(res.getValues());
      rowCount = Integer.valueOf(res.getValues().get(0).get(0));
      timeElapsedMs += sleepPeriodMs;
    }
    Assertions.assertEquals(expectedNumberOfRows.toString(),res.getValues().get(0).get(0));
  }
}
