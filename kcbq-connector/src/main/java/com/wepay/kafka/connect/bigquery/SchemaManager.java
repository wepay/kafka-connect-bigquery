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
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import com.wepay.kafka.connect.bigquery.api.TopicAndRecordName;
import com.wepay.kafka.connect.bigquery.convert.KafkaDataBuilder;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.ArrayList;
import java.util.Optional;

/**
 * Class for managing Schemas of BigQuery tables (creating and updating).
 */
public class SchemaManager {
  private static final Logger logger = LoggerFactory.getLogger(SchemaManager.class);

  private final SchemaRetriever schemaRetriever;
  private final SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter;
  private final BigQuery bigQuery;
  private final BigQuerySinkConfig config;
  private final Optional<String> kafkaKeyFieldName;
  private final Optional<String> kafkaDataFieldName;

  /**
   * @param bigQuery Used to communicate create/update requests to BigQuery.
   * @param config BigQuery sink configuration.
   */
  public SchemaManager(
      BigQuery bigQuery,
      BigQuerySinkConfig config) {
    this.schemaRetriever = config.getSchemaRetriever();
    this.schemaConverter = config.getSchemaConverter();
    this.bigQuery = bigQuery;
    this.config = config;
    this.kafkaKeyFieldName = config.getKafkaKeyFieldName();
    this.kafkaDataFieldName = config.getKafkaDataFieldName();
  }

  /**
   * Create a new table in BigQuery.
   * @param table The BigQuery table to create.
   * @param topicAndRecordName The Kafka topic and an optional record name used to determine the schema.
   */
  public void createTable(TableId table, TopicAndRecordName topicAndRecordName) {
    Schema kafkaValueSchema = schemaRetriever.retrieveSchema(table, topicAndRecordName, KafkaSchemaRecordType.VALUE);
    Schema kafkaKeySchema = kafkaKeyFieldName.isPresent() ? schemaRetriever.retrieveSchema(table, topicAndRecordName, KafkaSchemaRecordType.KEY) : null;
    bigQuery.create(constructTableInfo(table, kafkaKeySchema, kafkaValueSchema));
  }

  /**
   * Update an existing table in BigQuery.
   * @param table The BigQuery table to update.
   * @param topicAndRecordName The Kafka topic and an optional record name used to determine the schema.
   */
  public void updateSchema(TableId table, TopicAndRecordName topicAndRecordName) {
    Schema kafkaValueSchema = schemaRetriever.retrieveSchema(table, topicAndRecordName, KafkaSchemaRecordType.VALUE);
    Schema kafkaKeySchema = kafkaKeyFieldName.isPresent() ? schemaRetriever.retrieveSchema(table, topicAndRecordName, KafkaSchemaRecordType.KEY) : null;
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

  public Map<TopicAndRecordName, Schema> discoverSchemas() {
    List<String> topics = config.getList(BigQuerySinkConfig.TOPICS_CONFIG);
    Map<Pattern, String> recordAliases =
        config
            .getSinglePatterns(BigQuerySinkConfig.RECORD_ALIASES_CONFIG)
            .stream()
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    return schemaRetriever.retrieveSchemas(topics, recordAliases);
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
