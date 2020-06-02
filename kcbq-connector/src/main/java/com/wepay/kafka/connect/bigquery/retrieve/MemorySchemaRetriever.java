package com.wepay.kafka.connect.bigquery.retrieve;

import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MemorySchemaRetriever implements SchemaRetriever {
  private static final Logger logger = LoggerFactory.getLogger(MemorySchemaRetriever.class);
  private static final int CACHE_SIZE = 1000;
  private Cache<String, Schema> schemaCache;

  private String getCacheKey(String tableName, String topic, KafkaSchemaRecordType schemaType) {
    return String.format("%s-%s-%s", tableName, topic, schemaType.toString());
  }

  @Override
  public void configure(Map<String, String> properties) {
    schemaCache = new SynchronizedCache<>(new LRUCache<>(CACHE_SIZE));
  }

  @Override
  public Schema retrieveSchema(TableId table, String topic, KafkaSchemaRecordType schemaType) {
    String tableName = table.getTable();
    Schema schema = schemaCache.get(getCacheKey(tableName, topic, schemaType));
    if (schema != null) {
      return schema;
    }

    // By returning an empty schema the calling code will create a table without a schema.
    // When we receive our first message and try to add it, we'll hit the invalid schema case
    // and update the schema with the schema from the message
    return SchemaBuilder.struct().build();
  }

  // Keep this implementation in order to preserve backwards compatibility for older versions of the
  // connector.
  @Override
  public void setLastSeenSchema(TableId tableId, String topic, Schema schema) {
    setLastSeenSchema(tableId, topic, schema, KafkaSchemaRecordType.VALUE);
  }

  @Override
  public void setLastSeenSchema(TableId table, String topic, Schema schema, KafkaSchemaRecordType schemaType) {
    logger.debug("Updating last seen schema to " + schema.toString());
    schemaCache.put(getCacheKey(table.getTable(), topic, schemaType), schema);
  }
}
