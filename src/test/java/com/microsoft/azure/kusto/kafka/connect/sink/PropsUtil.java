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
    props.put("kusto.url","https://ingest-azureconnector.centralus.kusto.windows.net/");
    props.put("kusto.auth.authority","7f66d0ea-6137-4e37-a835-4530eba9b3ee");
    props.put("kusto.auth.appid","e299f8c0-4965-4b1f-9b55-a56dd4b8f6c4");
    props.put("kusto.auth.appkey","1t-fP8o~a0b8X7-o0.wCr6.0aPBR7~k9L");
    props.put("kusto.sink.tempdir","/var/tmp/");
    props.put("value.converter","org.apache.kafka.connect.storage.StringConverter");
    props.put("key.converter","org.apache.kafka.connect.storage.StringConverter");
    props.put("value.converter.schemas.enable","false");
    props.put("key.converter.schemas.enable","false");
    return props;
  }
}
