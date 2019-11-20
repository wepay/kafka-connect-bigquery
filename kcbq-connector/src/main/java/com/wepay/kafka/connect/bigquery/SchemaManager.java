package com.wepay.kafka.connect.bigquery;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;

import com.google.cloud.bigquery.TimePartitioning.Type;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class for managing Schemas of BigQuery tables (creating and updating).
 */
public class SchemaManager {
  private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);

  private final SchemaRetriever schemaRetriever;
  private final SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter;
  private final BigQuery bigQuery;
  private final String timestampPartitionFieldName;

  /**
   * @param schemaRetriever Used to determine the Kafka Connect Schema that should be used for a
   *                        given table.
   * @param schemaConverter Used to convert Kafka Connect Schemas into BigQuery format.
   * @param bigQuery Used to communicate create/update requests to BigQuery.
   * @param timestampPartitionFieldName the column name to use for timestamp partitioning.
   */
  public SchemaManager(
      SchemaRetriever schemaRetriever,
      SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter,
      BigQuery bigQuery,
      String timestampPartitionFieldName) {
    this.schemaRetriever = schemaRetriever;
    this.schemaConverter = schemaConverter;
    this.bigQuery = bigQuery;
    this.timestampPartitionFieldName = timestampPartitionFieldName;
  }

  /**
   * Create a new table in BigQuery.
   * @param table The BigQuery table to create.
   * @param topic The Kafka topic used to determine the schema.
   */
  public void createTable(TableId table, String topic) {
    Schema kafkaConnectSchema = schemaRetriever.retrieveSchema(table, topic);
    bigQuery.create(constructTableInfo(table, kafkaConnectSchema));
  }

  /**
   * Update an existing table in BigQuery.
   * @param table The BigQuery table to update.
   * @param topic The Kafka topic used to determine the schema.
   */
  public void updateSchema(TableId table, String topic) {
    Schema kafkaConnectSchema = schemaRetriever.retrieveSchema(table, topic);
    TableInfo tableInfo = constructTableInfo(table, kafkaConnectSchema);
    logger.info("Attempting to update table `{}` with schema {}",
        table, tableInfo.getDefinition().getSchema());
    bigQuery.update(tableInfo);
  }

  // package private for testing.
  TableInfo constructTableInfo(TableId table, Schema kafkaConnectSchema) {
    com.google.cloud.bigquery.Schema bigQuerySchema =
        schemaConverter.convertSchema(kafkaConnectSchema);

    TimePartitioning timePartitioning = TimePartitioning.of(Type.DAY);
    if (timestampPartitionFieldName != null || !timestampPartitionFieldName.isEmpty()) {
      // timestamp based partitioning vs ingestion time partitioning
      timePartitioning = timePartitioning.toBuilder().setField(timestampPartitionFieldName).build();
    }

    StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
        .setSchema(bigQuerySchema)
        .setTimePartitioning(timePartitioning)
        .build();
    TableInfo.Builder tableInfoBuilder =
        TableInfo.newBuilder(table, tableDefinition);
    if (kafkaConnectSchema.doc() != null) {
      tableInfoBuilder.setDescription(kafkaConnectSchema.doc());
    }
    return tableInfoBuilder.build();
  }
}
