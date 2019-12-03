package com.wepay.kafka.connect.bigquery.api;

import com.google.cloud.bigquery.TableId;

import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Interface for retrieving the most up-to-date schemas for a given BigQuery table. Used in
 * automatic table creation and schema updates.
 */
public interface SchemaRetriever {
  /**
   * Called with all of the configuration settings passed to the connector via its
   * {@link org.apache.kafka.connect.sink.SinkConnector#start(Map)} method.
   * @param properties The configuration settings of the connector.
   */
  void configure(Map<String, String> properties);

  /**
   * Retrieve the most current schema for the given topic.
   * @param table The table that will be created.
   * @param topicAndRecordName The topic and an optional record name to retrieve a schema for.
   * @return The Schema for the given table.
   */
  Schema retrieveSchema(TableId table, TopicAndRecordName topicAndRecordName);

  Map<TopicAndRecordName, Schema> retrieveSchemas(List<String> topics, Map<Pattern, String> recordAliases);

  /**
   * Set the last seen schema for a given topic
   * @param table The table that will be created.
   * @param topicAndRecordName The topic and an optional record name to retrieve a schema for.
   * @param schema The last seen Kafka Connect Schema
   */
  void setLastSeenSchema(TableId table, TopicAndRecordName topicAndRecordName, Schema schema);
}
