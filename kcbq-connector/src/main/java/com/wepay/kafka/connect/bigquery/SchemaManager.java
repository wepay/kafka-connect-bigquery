package com.wepay.kafka.connect.bigquery;


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.convert.KafkaDataBuilder;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Class for managing Schemas of BigQuery tables (creating and updating).
 */
public class SchemaManager {
  private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);

  private final SchemaRetriever schemaRetriever;
  private final SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter;
  private final BigQuery bigQuery;
  private final Optional<String> kafkaKeyFieldName;
  private final Optional<String> kafkaDataFieldName;

  /**
   * @param schemaRetriever Used to determine the Kafka Connect Schema that should be used for a
   *                        given table.
   * @param schemaConverter Used to convert Kafka Connect Schemas into BigQuery format.
   * @param bigQuery Used to communicate create/update requests to BigQuery.
   * @param kafkaKeyFieldName The name of kafka key field to be used in BigQuery.
   *                         If set to null, Kafka Key Field will not be included in BigQuery.
   * @param kafkaDataFieldName The name of kafka data field to be used in BigQuery.
   *                           If set to null, Kafka Data Field will not be included in BigQuery.
   */
  public SchemaManager(
      SchemaRetriever schemaRetriever,
      SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter,
      BigQuery bigQuery,
      Optional<String> kafkaKeyFieldName,
      Optional<String> kafkaDataFieldName) {
    this.schemaRetriever = schemaRetriever;
    this.schemaConverter = schemaConverter;
    this.bigQuery = bigQuery;
    this.kafkaKeyFieldName = kafkaKeyFieldName;
    this.kafkaDataFieldName = kafkaDataFieldName;
  }

  /**
   * Create a new table in BigQuery.
   * @param table The BigQuery table to create.
   * @param topic The Kafka topic used to determine the schema.
   */
  public void createTable(TableId table, String topic) {
    Schema kafkaValueSchema = schemaRetriever.retrieveSchema(table, topic, KafkaSchemaRecordType.VALUE);
    Schema kafkaKeySchema = kafkaKeyFieldName.isPresent() ? schemaRetriever.retrieveSchema(table, topic, KafkaSchemaRecordType.KEY) : null;
    bigQuery.create(constructTableInfo(table, kafkaKeySchema, kafkaValueSchema));
  }

  /**
   * Update an existing table in BigQuery.
   * @param table The BigQuery table to update.
   * @param topic The Kafka topic used to determine the schema.
   */
  public void updateSchema(TableId table, String topic) {
    Schema kafkaValueSchema = schemaRetriever.retrieveSchema(table, topic, KafkaSchemaRecordType.VALUE);
    Schema kafkaKeySchema = kafkaKeyFieldName.isPresent() ? schemaRetriever.retrieveSchema(table, topic, KafkaSchemaRecordType.KEY) : null;
    TableInfo tableInfo = constructTableInfo(table, kafkaKeySchema, kafkaValueSchema);
    logger.info("Attempting to update table `{}` with schema {}",
        table, tableInfo.getDefinition().getSchema());
    bigQuery.update(tableInfo);
  }

  // package private for testing.
  TableInfo constructTableInfo(TableId table, Schema kafkaKeySchema, Schema kafkaValueSchema) {
    com.google.cloud.bigquery.Schema bigQuerySchema = getBigQuerySchema(kafkaKeySchema, kafkaValueSchema);
    StandardTableDefinition tableDefinition = StandardTableDefinition.newBuilder()
        .setSchema(bigQuerySchema)
        .setTimePartitioning(TimePartitioning.of(TimePartitioning.Type.DAY))
        .build();
    TableInfo.Builder tableInfoBuilder =
        TableInfo.newBuilder(table, tableDefinition);
    if (kafkaValueSchema.doc() != null) {
      tableInfoBuilder.setDescription(kafkaValueSchema.doc());
    }
    return tableInfoBuilder.build();
  }

  private com.google.cloud.bigquery.Schema getBigQuerySchema(Schema kafkaKeySchema, Schema kafkaValueSchema) {
      List<Field> allFields = new ArrayList<> ();
      com.google.cloud.bigquery.Schema valueSchema = schemaConverter.convertSchema(kafkaValueSchema);
      allFields.addAll(valueSchema.getFields());
      if (kafkaKeyFieldName.isPresent()) {
          com.google.cloud.bigquery.Schema keySchema = schemaConverter.convertSchema(kafkaKeySchema);
          Field kafkaKeyField = Field.newBuilder(kafkaKeyFieldName.get(), LegacySQLTypeName.RECORD, keySchema.getFields())
                  .setMode(Field.Mode.NULLABLE).build();
          allFields.add(kafkaKeyField);
      }
      if (kafkaDataFieldName.isPresent()) {
          Field kafkaDataField = KafkaDataBuilder.buildKafkaDataField(kafkaDataFieldName.get());
          allFields.add(kafkaDataField);
      }
      return com.google.cloud.bigquery.Schema.of(allFields);
  }

}
