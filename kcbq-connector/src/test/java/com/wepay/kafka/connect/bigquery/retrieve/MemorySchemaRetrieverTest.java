package com.wepay.kafka.connect.bigquery.retrieve;

import com.google.cloud.bigquery.TableId;

import com.google.common.collect.Maps;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import com.wepay.kafka.connect.bigquery.api.TopicAndRecordName;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class MemorySchemaRetrieverTest {
  public TableId getTableId(String datasetName, String tableName) {
    return TableId.of(datasetName, tableName);
  }

  @Test
  public void testRetrieveSchemaWhenNoLastSeenSchemaReturnsEmptyStructSchema() {
    final TopicAndRecordName topic = TopicAndRecordName.from("test-retrieve", "test-record");
    final TableId tableId = getTableId("testTable", "testDataset");
    SchemaRetriever retriever = new MemorySchemaRetriever();
    retriever.configure(new HashMap<>());
    Assert.assertEquals(retriever.retrieveSchema(tableId, topic, KafkaSchemaRecordType.VALUE), SchemaBuilder.struct().build());
  }

  @Test
  public void testRetrieveSchemaWhenLastSeenExistsSucceeds() {
    final TopicAndRecordName topic = TopicAndRecordName.from("test-retrieve", "test-record");
    final TableId tableId = getTableId("testTable", "testDataset");
    SchemaRetriever retriever = new MemorySchemaRetriever();
    retriever.configure(new HashMap<>());

    Schema expectedSchema = Schema.OPTIONAL_FLOAT32_SCHEMA;
    retriever.setLastSeenSchema(tableId, topic, expectedSchema);

    Assert.assertEquals(retriever.retrieveSchema(tableId, topic, KafkaSchemaRecordType.KEY), expectedSchema);
  }

  @Test
  public void testRetrieveSchemaWithMultipleSchemasSucceeds() {
    final TopicAndRecordName floatSchemaTopic = TopicAndRecordName.from("test-float32", "float32-record-name");
    final TopicAndRecordName intSchemaTopic = TopicAndRecordName.from("test-int32", "int32-record-name");
    final TableId floatTableId = getTableId("testFloatDataset", "testFloatTable");
    final TableId intTableId = getTableId("testIntDataset", "testIntTable");
    SchemaRetriever retriever = new MemorySchemaRetriever();
    retriever.configure(new HashMap<>());

    Schema expectedIntSchema = Schema.INT32_SCHEMA;
    Schema expectedFloatSchema = Schema.OPTIONAL_FLOAT32_SCHEMA;
    retriever.setLastSeenSchema(floatTableId, floatSchemaTopic, expectedFloatSchema);
    retriever.setLastSeenSchema(intTableId, intSchemaTopic, expectedIntSchema);

    Assert.assertEquals(
        retriever.retrieveSchema(floatTableId, floatSchemaTopic, KafkaSchemaRecordType.KEY), expectedFloatSchema);
    Assert.assertEquals(retriever.retrieveSchema(intTableId, intSchemaTopic, KafkaSchemaRecordType.KEY), expectedIntSchema);
  }

  @Test
  public void testRetrieveSchemasSucceeds() {
    final TopicAndRecordName floatSchemaTopic = TopicAndRecordName.from("test-float32", "float32-record-name");
    final TopicAndRecordName intSchemaTopic = TopicAndRecordName.from("test-int32", "int32-record-name");
    final TableId floatTableId = getTableId("testFloatTable", "testFloatDataset");
    final TableId intTableId = getTableId("testIntTable", "testIntDataset");
    SchemaRetriever retriever = new MemorySchemaRetriever();
    retriever.configure(new HashMap<>());

    Schema expectedIntSchema = SchemaBuilder.struct().name("schemaA").build();
    Schema expectedFloatSchema = SchemaBuilder.struct().optional().name("schemaB").build();

    retriever.setLastSeenSchema(floatTableId, floatSchemaTopic, expectedFloatSchema);
    retriever.setLastSeenSchema(intTableId, intSchemaTopic, expectedIntSchema);

    Map<TopicAndRecordName, Schema> expectedSchemas = Maps.newHashMap();
    expectedSchemas.put(floatSchemaTopic, expectedFloatSchema);
    expectedSchemas.put(intSchemaTopic, expectedIntSchema);

    List<String> topics = Stream.of(floatSchemaTopic.getTopic(), intSchemaTopic.getTopic()).collect(Collectors.toList());
    Map<Pattern, String> recordAliases = Collections.emptyMap();

    Assert.assertEquals(retriever.retrieveSchemas(topics, recordAliases), expectedSchemas);
  }

  @Test
  public void testRetrieveSchemaRetrievesLastSeenSchema() {
    final TopicAndRecordName intSchemaTopic = TopicAndRecordName.from("test-int32", "int32-record-name");
    final TableId tableId = getTableId("testTable", "testDataset");
    SchemaRetriever retriever = new MemorySchemaRetriever();
    retriever.configure(new HashMap<>());

    Schema firstSchema = Schema.INT32_SCHEMA;
    Schema secondSchema = Schema.INT64_SCHEMA;
    retriever.setLastSeenSchema(tableId, intSchemaTopic, firstSchema);
    retriever.setLastSeenSchema(tableId, intSchemaTopic, secondSchema);

    Assert.assertEquals(retriever.retrieveSchema(tableId, intSchemaTopic, KafkaSchemaRecordType.VALUE), secondSchema);
  }
}
