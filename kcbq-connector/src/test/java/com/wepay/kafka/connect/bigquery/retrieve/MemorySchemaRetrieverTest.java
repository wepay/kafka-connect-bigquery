package com.wepay.kafka.connect.bigquery.retrieve;


import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;


public class MemorySchemaRetrieverTest {
  public TableId getTableId(String datasetName, String tableName) {
    return TableId.of(datasetName, tableName);
  }

  @Test
  public void testRetrieveSchemaWhenNoLastSeenSchemaReturnsEmptyStructSchema() {
    final String topic = "test-retrieve";
    final TableId tableId = getTableId("testTable", "testDataset");
    SchemaRetriever retriever = new MemorySchemaRetriever();
    retriever.configure(new HashMap<>());
    Assert.assertEquals(SchemaBuilder.struct().build(), retriever.retrieveSchema(tableId, topic, KafkaSchemaRecordType.VALUE));
  }

  @Test
  public void testRetrieveSchemaWhenLastSeenExistsSucceeds() {
    final String topic = "test-retrieve";
    final TableId tableId = getTableId("testTable", "testDataset");
    SchemaRetriever retriever = new MemorySchemaRetriever();
    retriever.configure(new HashMap<>());

    Schema expectedSchema = Schema.OPTIONAL_FLOAT32_SCHEMA;
    retriever.setLastSeenSchema(tableId, topic, expectedSchema);

    Assert.assertEquals(expectedSchema, retriever.retrieveSchema(tableId, topic, KafkaSchemaRecordType.VALUE));
  }

  @Test
  public void testRetrieveSchemaWithMultipleSchemasSucceeds() {
    final String floatSchemaTopic = "test-float32";
    final String intSchemaTopic = "test-int32";
    final TableId floatTableId = getTableId("testFloatTable", "testFloatDataset");
    final TableId intTableId = getTableId("testIntTable", "testIntDataset");
    SchemaRetriever retriever = new MemorySchemaRetriever();
    retriever.configure(new HashMap<>());

    Schema expectedIntSchema = Schema.INT32_SCHEMA;
    Schema expectedFloatSchema = Schema.OPTIONAL_FLOAT32_SCHEMA;
    retriever.setLastSeenSchema(floatTableId, floatSchemaTopic, expectedFloatSchema);
    retriever.setLastSeenSchema(intTableId, intSchemaTopic, expectedIntSchema);

    Assert.assertEquals(expectedFloatSchema,
        retriever.retrieveSchema(floatTableId, floatSchemaTopic, KafkaSchemaRecordType.VALUE));
    Assert.assertEquals(expectedIntSchema,
        retriever.retrieveSchema(intTableId, intSchemaTopic, KafkaSchemaRecordType.VALUE));
  }

  @Test
  public void testRetrieveSchemaRetrievesLastSeenSchema() {
    final String intSchemaTopic = "test-int";
    final TableId tableId = getTableId("testTable", "testDataset");
    SchemaRetriever retriever = new MemorySchemaRetriever();
    retriever.configure(new HashMap<>());

    Schema firstSchema = Schema.INT32_SCHEMA;
    Schema secondSchema = Schema.INT64_SCHEMA;
    retriever.setLastSeenSchema(tableId, intSchemaTopic, firstSchema);
    retriever.setLastSeenSchema(tableId, intSchemaTopic, secondSchema);

    Assert.assertEquals(secondSchema,
        retriever.retrieveSchema(tableId, intSchemaTopic, KafkaSchemaRecordType.VALUE));
  }

  @Test
  public void testRetrieveKeyAndValueSchema() {
    final String schemaTopic = "test-key-and-value";
    final TableId tableId = getTableId("testTable", "testDataset");
    SchemaRetriever retriever = new MemorySchemaRetriever();
    retriever.configure(new HashMap<>());

    Schema keySchema = Schema.STRING_SCHEMA;
    Schema valueSchema = Schema.OPTIONAL_BOOLEAN_SCHEMA;
    retriever.setLastSeenSchema(tableId, schemaTopic, keySchema, KafkaSchemaRecordType.KEY);
    retriever.setLastSeenSchema(tableId, schemaTopic, valueSchema, KafkaSchemaRecordType.VALUE);

    Assert.assertEquals(keySchema,
        retriever.retrieveSchema(tableId, schemaTopic, KafkaSchemaRecordType.KEY));
    Assert.assertEquals(valueSchema,
        retriever.retrieveSchema(tableId, schemaTopic, KafkaSchemaRecordType.VALUE));
  }
}
