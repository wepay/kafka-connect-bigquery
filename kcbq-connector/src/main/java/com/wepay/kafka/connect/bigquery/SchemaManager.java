package com.wepay.kafka.connect.bigquery;


import com.google.cloud.bigquery.*;

import com.google.cloud.bigquery.TimePartitioning.Type;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import com.wepay.kafka.connect.bigquery.convert.KafkaDataBuilder;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Class for managing Schemas of BigQuery tables (creating and updating).
 */
public class SchemaManager {
  private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);

  private final SchemaRetriever schemaRetriever;
  private final SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter;
  private final BigQuery bigQuery;
  private final boolean autoAddNewFields;
  private final boolean autoChangeReqToNullable;
  private final Optional<String> kafkaKeyFieldName;
  private final Optional<String> kafkaDataFieldName;
  private final Optional<String> timestampPartitionFieldName;
  private final Optional<List<String>> clusteringFieldName;


  /**
   * @param schemaRetriever Used to determine the Kafka Connect Schema that should be used for a
   *                        given table.
   * @param schemaConverter Used to convert Kafka Connect Schemas into BigQuery format.
   * @param bigQuery Used to communicate create/update requests to BigQuery.
   * @param autoAddNewFields If set to true, allows new fields to be added to BigQuery Schema.
   * @param changeReqToNullable If set to true, allows changing field mode from REQUIRED to NULLABLE
   * @param kafkaKeyFieldName The name of kafka key field to be used in BigQuery.
   *                         If set to null, Kafka Key Field will not be included in BigQuery.
   * @param kafkaDataFieldName The name of kafka data field to be used in BigQuery.
   *                           If set to null, Kafka Data Field will not be included in BigQuery.
   */
  public SchemaManager(
      SchemaRetriever schemaRetriever,
      SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter,
      BigQuery bigQuery,
      boolean autoAddNewFields,
      boolean changeReqToNullable,
      Optional<String> kafkaKeyFieldName,
      Optional<String> kafkaDataFieldName,
      Optional<String> timestampPartitionFieldName,
      Optional<List<String>> clusteringFieldName) {
    this.schemaRetriever = schemaRetriever;
    this.schemaConverter = schemaConverter;
    this.bigQuery = bigQuery;
    this.autoAddNewFields = autoAddNewFields;
    this.autoChangeReqToNullable = changeReqToNullable;
    this.kafkaKeyFieldName = kafkaKeyFieldName;
    this.kafkaDataFieldName = kafkaDataFieldName;
    this.timestampPartitionFieldName = timestampPartitionFieldName;
    this.clusteringFieldName = clusteringFieldName;
  }

  /**
   * Create a new table in BigQuery.
   * @param table The BigQuery table to create.
   * @param records The sink records used to determine the schema.
   */
  public void createTable(TableId table, Set<SinkRecord> records) {
    List<com.google.cloud.bigquery.Schema> bigQuerySchemas = new ArrayList<>();
    String tableDescription = null;
    for (SinkRecord record: records) {
      Schema kafkaValueSchema = schemaRetriever.retrieveValueSchema(record);
      Schema kafkaKeySchema = kafkaKeyFieldName.isPresent() ? schemaRetriever.retrieveKeySchema(record) : null;
      tableDescription = (kafkaValueSchema.doc() != null) ? kafkaValueSchema.doc() : tableDescription;
      com.google.cloud.bigquery.Schema schema = getBigQuerySchema(kafkaKeySchema, kafkaValueSchema);
      bigQuerySchemas.add(schema);
    }
    com.google.cloud.bigquery.Schema schema = getUnionizedSchema(bigQuerySchemas);
    bigQuery.create(constructTableInfo(table, schema, tableDescription));
  }

  /**
   * Update an existing table in BigQuery.
   * @param table The BigQuery table to update.
   * @param records The sink records used to update the schema.
   */
  public void updateSchema(TableId table, Set<SinkRecord> records) {
    List<com.google.cloud.bigquery.Schema> bigQuerySchemas = new ArrayList<>();
    String tableDescription = null;
    Table bigQueryTable = bigQuery.getTable(table.getDataset(), table.getTable());
    com.google.cloud.bigquery.Schema bigQueryTableSchema = bigQueryTable.getDefinition().getSchema();
    bigQuerySchemas.add(bigQueryTableSchema);
    for (SinkRecord record: records) {
      Schema kafkaValueSchema = schemaRetriever.retrieveValueSchema(record);
      Schema kafkaKeySchema = kafkaKeyFieldName.isPresent() ? schemaRetriever.retrieveKeySchema(record) : null;
      tableDescription = (kafkaValueSchema.doc() != null) ? kafkaValueSchema.doc() : tableDescription;
      com.google.cloud.bigquery.Schema schema = getBigQuerySchema(kafkaKeySchema, kafkaValueSchema);
      bigQuerySchemas.add(schema);
    }
    com.google.cloud.bigquery.Schema schema = getUnionizedSchema(bigQuerySchemas);
    TableInfo tableInfo = constructTableInfo(table, schema, tableDescription);
    logger.info("Attempting to update table `{}` with schema {}",
            table, tableInfo.getDefinition().getSchema());
    bigQuery.update(tableInfo);
  }

  private com.google.cloud.bigquery.Schema getUnionizedSchema(List<com.google.cloud.bigquery.Schema> schemas){
    com.google.cloud.bigquery.Schema currentSchema = schemas.get(0);
    for (int i=1; i < schemas.size(); i++) {
      currentSchema = unionizeSchemas(currentSchema, schemas.get(i));
    }
    return currentSchema;
  }
  private com.google.cloud.bigquery.Schema unionizeSchemas(com.google.cloud.bigquery.Schema currentSchema, com.google.cloud.bigquery.Schema newSchema){
    Map<String, Field> currentFields = new HashMap<>();
    Map<String, Field> newFields = new HashMap<>();
    FieldList currentSchemaFieldList = currentSchema.getFields();
    FieldList newSchemaFieldList = newSchema.getFields();
    for (Field field: currentSchemaFieldList) {
      currentFields.put(field.getName(), field);
    }
    for (Field field: newSchemaFieldList) {
      newFields.put(field.getName(), field);
    }
    for (Map.Entry<String, Field> entry: newFields.entrySet()) {
      if (!currentFields.containsKey(entry.getKey())) {
        if (autoAddNewFields) {
          currentFields.put(entry.getKey(), entry.getValue().toBuilder().setMode(Field.Mode.NULLABLE).build());
        } else {
          throw new BigQueryConnectException("No field with the name " + entry.getKey() );
        }
      } else {
        if (currentFields.get(entry.getKey()).getType() != newFields.get(entry.getKey()).getType()) {
          throw new BigQueryConnectException("Incompatible Schema: Field Type Mismatch");
        }
        if (currentFields.get(entry.getKey()).getMode() == Field.Mode.REQUIRED && newFields.get(entry.getKey()).getMode() == Field.Mode.NULLABLE){
          if (autoChangeReqToNullable) {
            currentFields.put(entry.getKey(), entry.getValue().toBuilder().setMode(Field.Mode.NULLABLE).build());
          } else {
            throw new BigQueryConnectException("Missing Required Field. " + entry.getKey() + " has mode REQUIRED");
          }
        }
      }
    }
    return com.google.cloud.bigquery.Schema.of(currentFields.values());
  }

  // package private for testing.
  TableInfo constructTableInfo(TableId table, com.google.cloud.bigquery.Schema bigQuerySchema, String tableDescription) {
    TimePartitioning timePartitioning = TimePartitioning.of(Type.DAY);
    if (timestampPartitionFieldName.isPresent()) {
      timePartitioning = timePartitioning.toBuilder().setField(timestampPartitionFieldName.get()).build();
    }

    StandardTableDefinition.Builder builder = StandardTableDefinition.newBuilder()
        .setSchema(bigQuerySchema)
        .setTimePartitioning(timePartitioning);

    if (timestampPartitionFieldName.isPresent() && clusteringFieldName.isPresent()) {
      Clustering clustering = Clustering.newBuilder()
          .setFields(clusteringFieldName.get())
          .build();
      builder.setClustering(clustering);
    }

    StandardTableDefinition tableDefinition = builder.build();
    TableInfo.Builder tableInfoBuilder =
        TableInfo.newBuilder(table, tableDefinition);
    if (tableDescription != null) {
      tableInfoBuilder.setDescription(tableDescription);
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
