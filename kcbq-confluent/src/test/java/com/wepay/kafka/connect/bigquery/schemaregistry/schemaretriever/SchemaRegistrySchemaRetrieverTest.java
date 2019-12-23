package com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.TableId;

import com.google.common.collect.Sets;

import com.wepay.kafka.connect.bigquery.api.TopicAndRecordName;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import io.confluent.connect.avro.AvroData;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import org.junit.Test;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SchemaRegistrySchemaRetrieverTest {
  @Test
  public void testRetrieveSchema() throws Exception {
    final TableId table = TableId.of("test", "kafka_topic");
    final String testTopic = "kafka-topic";
    final String testRecordName = "testrecord";
    final TopicAndRecordName testTopicAndRecordName = TopicAndRecordName.from(testTopic, testRecordName);
    final SchemaMetadata testSchemaMetadata = testSchemaMetadata(testRecordName);

    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    when(schemaRegistryClient.getLatestSchemaMetadata(testTopicAndRecordName.toSubject(KafkaSchemaRecordType.VALUE))).thenReturn(testSchemaMetadata);
    when(schemaRegistryClient.getLatestSchemaMetadata(testTopicAndRecordName.toSubject(KafkaSchemaRecordType.KEY))).thenReturn(testSchemaMetadata);

    SchemaRegistrySchemaRetriever testSchemaRetriever = new SchemaRegistrySchemaRetriever(
        schemaRegistryClient,
        new AvroData(0)
    );

    Schema expectedKafkaConnectSchema = schemaFor(testRecordName);

    assertEquals(expectedKafkaConnectSchema, testSchemaRetriever.retrieveSchema(table, testTopicAndRecordName, KafkaSchemaRecordType.VALUE));
    assertEquals(expectedKafkaConnectSchema, testSchemaRetriever.retrieveSchema(table, testTopicAndRecordName, KafkaSchemaRecordType.KEY));
  }

  @Test
  public void testRetrieveMultipleSchemas() throws Exception {
    final List<String> testTopics = Stream.of("topic-1", "topic-2").collect(Collectors.toList());
    final Map<String, String> subjectPatternsToRecords = Collections.unmodifiableMap(Stream.of(
        new SimpleEntry<>("topic-1-record.A", "record.A"),
        new SimpleEntry<>("topic-1-not.a.record.A", "record.A"),
        new SimpleEntry<>("topic-2-record.B", "record.B"),
        new SimpleEntry<>("topic-2-not.a.record.B", "record.B"),
        new SimpleEntry<>("topic-3-record.A", "record.A"),
        new SimpleEntry<>("topic-2-record.C", "record.C"),
        new SimpleEntry<>("topic-3-record.C", "record.C"),
        new SimpleEntry<>("justTopic-record.A", "record.A"),
        new SimpleEntry<>("topic-1-justRecord", "justRecord")
    ).collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue)));

    Map<Pattern, String> testRecordAliases = new HashMap<>();
    testRecordAliases.put(Pattern.compile("record\\.A"), "recordA");
    testRecordAliases.put(Pattern.compile("rec.*\\.B"), "recordB");
    testRecordAliases.put(Pattern.compile(".*\\.C"), "recordC");
    testRecordAliases = Collections.unmodifiableMap(testRecordAliases);

    SchemaRegistryClient schemaRegistryClient = mock(SchemaRegistryClient.class);
    when(schemaRegistryClient.getAllSubjects()).thenReturn(subjectPatternsToRecords.keySet());
    for (Map.Entry<String, String> subjectToRecordName : subjectPatternsToRecords.entrySet()) {
      when(schemaRegistryClient.getLatestSchemaMetadata(subjectToRecordName.getKey())).thenReturn(testSchemaMetadata(subjectToRecordName.getValue()));
    }

    SchemaRegistrySchemaRetriever testSchemaRetriever = new SchemaRegistrySchemaRetriever(
        schemaRegistryClient,
        new AvroData(0)
    );

    List<Schema> expectedKafkaConnectSchemas = Stream.of(
        schemaFor("record.A"),
        schemaFor("record.B"),
        schemaFor("record.C")
    ).collect(Collectors.toList());

    Map<TopicAndRecordName, Schema> retrievedSchemas = testSchemaRetriever.retrieveSchemas(testTopics, testRecordAliases);
    assertThat(retrievedSchemas.size(), is(3));
    assertThat(Sets.newHashSet(retrievedSchemas.values()), equalTo(Sets.newHashSet(expectedKafkaConnectSchemas)));
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
