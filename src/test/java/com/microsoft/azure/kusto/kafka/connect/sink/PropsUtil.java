package com.microsoft.azure.kusto.kafka.connect.sink;

import java.util.HashMap;
import java.util.Map;

public class PropsUtil {
  protected static Map<String, String> getProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("connector.class", "com.microsoft.azure.kusto.kafka.connect.sink.KustoSinkConnector");
    props.put("topics","mytopic");
    props.put("tasks.max","1");
    props.put("kusto.tables.topics_mapping","[{'topic': 'mytopic','db': 'TestConnectorDB', 'table': 'SanchayTable','format': 'json', 'mapping':'SanchayMapping'}]");
    props.put("kusto.url","");
    props.put("kusto.auth.authority","");
    props.put("kusto.auth.appid","");
    props.put("kusto.auth.appkey","");
    props.put("kusto.sink.tempdir","/var/tmp/");
    props.put("value.converter","org.apache.kafka.connect.storage.StringConverter");
    props.put("key.converter","org.apache.kafka.connect.storage.StringConverter");
    props.put("value.converter.schemas.enable","false");
    props.put("key.converter.schemas.enable","false");
    return props;
  }
}
