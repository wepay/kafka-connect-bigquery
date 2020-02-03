package com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever;

import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.api.TopicAndRecordName;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import io.confluent.connect.avro.AvroData;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import org.junit.Test;

public class SchemaRegistrySchemaRetrieverTest {
  @Test
  public void testRetrieveSchema() throws Exception {
    final TableId table = TableId.of("test", "kafka_topic");
    final String testTopic = "kafka-topic";
    final String testRecordName = "testrecord";
    final TopicAndRecordName testTopicAndRecordName = TopicAndRecordName.from(testTopic, testRecordName);
    final String schemaRegistrySubjectName = testTopic + '-' + testRecordName;
    final SchemaMetadata testSchemaMetadata = testSchemaMetadata(testRecordName);

    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    when(schemaRegistryClient.getLatestSchemaMetadata(schemaRegistrySubjectName)).thenReturn(testSchemaMetadata);
    when(schemaRegistryClient.getLatestSchemaMetadata(schemaRegistrySubjectName)).thenReturn(testSchemaMetadata);

    SchemaRegistrySchemaRetriever testSchemaRetriever = new SchemaRegistrySchemaRetriever(
        schemaRegistryClient,
        new AvroData(0)
    );

    Schema expectedKafkaConnectSchema = schemaFor(testRecordName);

    assertEquals(expectedKafkaConnectSchema, testSchemaRetriever.retrieveSchema(table, testTopicAndRecordName, KafkaSchemaRecordType.VALUE));
    assertEquals(expectedKafkaConnectSchema, testSchemaRetriever.retrieveSchema(table, testTopicAndRecordName, KafkaSchemaRecordType.KEY));
  }

  private SchemaMetadata testSchemaMetadata(String schemaName) {
    String schemaString = "{\"type\": \"record\", "
        + "\"name\": \"" + schemaName + "\", "
        + "\"fields\": [{\"name\": \"f1\", \"type\": \"string\"}]}";
    return new SchemaMetadata(1, 1, schemaString);
  }

  private Schema schemaFor(String subject) {
    return SchemaBuilder.struct().field("f1", Schema.STRING_SCHEMA).name(subject).build();
  }

}
