package com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

/**
 * Class for managing the configuration properties of the Schema Registry Schema Retriever.
 */
public class SchemaRegistrySchemaRetrieverConfig extends AbstractConfig {
  private static final ConfigDef config;

  public static final String LOCATION_CONFIG =                     "schemaRegistryLocation";
  private static final ConfigDef.Type LOCATION_TYPE =              ConfigDef.Type.STRING;
  private static final ConfigDef.Importance LOCATION_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String LOCATION_DOC =
      "The base URL of the Schema Registry instance to use";

  public static final String BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG =                     "schemaRegistryBasicAuthCredentialsSource";
  private static final ConfigDef.Type BASIC_AUTH_CREDENTIALS_SOURCE_TYPE =              ConfigDef.Type.STRING;
  private static final ConfigDef.Importance BASIC_AUTH_CREDENTIALS_SOURCE_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String BASIC_AUTH_CREDENTIALS_SOURCE_DOC =
          "The Schema Registry basic auth credentials source";

  public static final String BASIC_AUTH_USER_INFO_CONFIG =                     "schemaRegistryBasicAuthUserInfo";
  private static final ConfigDef.Type BASIC_AUTH_USER_INFO_TYPE =              ConfigDef.Type.STRING;
  private static final ConfigDef.Importance BASIC_AUTH_USER_INFO_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String BASIC_AUTH_USER_INFO_DOC =
          "The Schema Registry basic auth user info";

  public static final String AVRO_DATA_CACHE_SIZE_CONFIG =                 "avroDataCacheSize";
  private static final ConfigDef.Type AVRO_DATA_CACHE_SIZE_TYPE =          ConfigDef.Type.INT;
  public static final Integer AVRO_DATA_CACHE_SIZE_DEFAULT =               100;
  private static final ConfigDef.Validator AVRO_DATA_CACHE_SIZE_VALIDATOR =
      ConfigDef.Range.atLeast(0);
  private static final ConfigDef.Importance AVRO_DATA_CACHE_SIZE_IMPORTANCE =
      ConfigDef.Importance.LOW;
  private static final String AVRO_DATA_CACHE_SIZE_DOC =
      "The size of the cache to use when converting schemas from Avro to Kafka Connect";

  static {
    config = new ConfigDef()
        .define(
            LOCATION_CONFIG,
            LOCATION_TYPE,
            LOCATION_IMPORTANCE,
            LOCATION_DOC
        ).define(
            BASIC_AUTH_CREDENTIALS_SOURCE_CONFIG,
            BASIC_AUTH_CREDENTIALS_SOURCE_TYPE,
            BASIC_AUTH_CREDENTIALS_SOURCE_IMPORTANCE,
            BASIC_AUTH_CREDENTIALS_SOURCE_DOC
        ).define(
            BASIC_AUTH_USER_INFO_CONFIG,
            BASIC_AUTH_USER_INFO_TYPE,
            BASIC_AUTH_USER_INFO_IMPORTANCE,
            BASIC_AUTH_USER_INFO_DOC
        ).define(
            AVRO_DATA_CACHE_SIZE_CONFIG,
            AVRO_DATA_CACHE_SIZE_TYPE,
            AVRO_DATA_CACHE_SIZE_DEFAULT,
            AVRO_DATA_CACHE_SIZE_VALIDATOR,
            AVRO_DATA_CACHE_SIZE_IMPORTANCE,
            AVRO_DATA_CACHE_SIZE_DOC
        );
  }

  /**
   * @param properties A Map detailing configuration properties and their respective values.
   */
  public SchemaRegistrySchemaRetrieverConfig(Map<String, String> properties) {
    super(config, properties);
  }
}
