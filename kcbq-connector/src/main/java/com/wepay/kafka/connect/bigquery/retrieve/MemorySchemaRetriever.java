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

/**
 * Uses the Confluent Schema Registry to fetch the latest schema for a given topic.
 */
public class MemorySchemaRetriever implements SchemaRetriever {
  private static final Logger logger = LoggerFactory.getLogger(MemorySchemaRetriever.class);
  private static final int CACHE_SIZE = 1000;
  private Cache<String, Schema> schemaCache;

  /**
   * Only here because the package-private constructor (which is only used in testing) would
   * otherwise cover up the no-args constructor.
   */
  public MemorySchemaRetriever() {
  }

  private String getCacheKey(String tableName, String topic) {
    return new StringBuilder(tableName).append(topic).toString();
  }

  @Override
  public void configure(Map<String, String> properties) {
    schemaCache = new SynchronizedCache<>(new LRUCache<String, Schema>(CACHE_SIZE));
  }

  @Override
  public Schema retrieveSchema(TableId table, String topic, KafkaSchemaRecordType schemaType) {
    String tableName = table.getTable();
    Schema schema = schemaCache.get(getCacheKey(tableName, topic));
    if (schema != null) {
      return schema;
    }

    // By returning an empty schema the calling code will create a table without a schema.
    // When we receive our first message and try to add it, we'll hit the invalid schema case
    // and update the schema with the schema from the message
    return SchemaBuilder.struct().build();
  }

  @Override
  public void setLastSeenSchema(TableId table, String topic, Schema schema) {
    logger.debug("Updating last seen schema to " + schema.toString());
    schemaCache.put(getCacheKey(table.getTable(), topic), schema);
  }
}
