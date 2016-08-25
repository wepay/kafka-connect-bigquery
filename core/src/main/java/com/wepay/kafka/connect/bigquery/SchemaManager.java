package com.wepay.kafka.connect.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import org.apache.kafka.connect.data.Schema;

import java.util.Set;

/**
 * Class for managing Schemas of BigQuery tables (creating and updating).
 */
public class SchemaManager {
  private final SchemaRetriever schemaRetriever;
  private final SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter;
  private final BigQuery bigQuery;

  /**
   * @param schemaRetriever Used to determine the Kafka Connect Schema that should be used for a
   *                        given table.
   * @param schemaConverter Used to convert Kafka Connect Schemas into BigQuery format.
   * @param bigQuery Used to communicate create/update requests to BigQuery.
   */
  public SchemaManager(
      SchemaRetriever schemaRetriever,
      SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter,
      BigQuery bigQuery) {
    this.schemaRetriever = schemaRetriever;
    this.schemaConverter = schemaConverter;
    this.bigQuery = bigQuery;
  }

  /**
   * Create a new table in BigQuery.
   * @param table The BigQuery table to create.
   * @param topic The Kafka topic used to determine the schema.
   */
  public void createTable(TableId table, String topic) {
    Schema kafkaConnectSchema = schemaRetriever.retrieveSchema(topic);
    bigQuery.create(constructTableInfo(table, kafkaConnectSchema));
  }

  /**
   * Update an existing table in BigQuery.
   * @param table The BigQuery table to update.
   * @param topic The Kafka topic used to determine the schema.
   * @param schemas The Kafka Connect schemas recently used in an attempt to write to BigQuery.
   */
  public void updateSchema(TableId table, String topic, Set<Schema> schemas) {
    Schema kafkaConnectSchema = schemaRetriever.retrieveSchema(topic, schemas);
    bigQuery.update(constructTableInfo(table, kafkaConnectSchema));
  }

  private TableInfo constructTableInfo(TableId table, Schema kafkaConnectSchema) {
    com.google.cloud.bigquery.Schema bigQuerySchema =
        schemaConverter.convertSchema(kafkaConnectSchema);
    return TableInfo.of(table, StandardTableDefinition.of(bigQuerySchema));
  }
}
