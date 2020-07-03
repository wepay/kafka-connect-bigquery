package com.wepay.kafka.connect.bigquery.config;

/*
 * Copyright 2016 WePay, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import com.google.cloud.bigquery.Schema;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import com.wepay.kafka.connect.bigquery.convert.BigQueryRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.BigQuerySchemaConverter;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Optional;

/**
 * Base class for connector and task configs; contains properties shared between the two of them.
 */
public class BigQuerySinkConfig extends AbstractConfig {
  private static final ConfigDef config;
  private static final Validator validator = new Validator();
  private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkConfig.class);

  // Values taken from https://github.com/apache/kafka/blob/1.1.1/connect/runtime/src/main/java/org/apache/kafka/connect/runtime/SinkConnectorConfig.java#L33
  public static final String TOPICS_CONFIG =                     SinkConnector.TOPICS_CONFIG;
  private static final ConfigDef.Type TOPICS_TYPE =              ConfigDef.Type.LIST;
  private static final ConfigDef.Importance TOPICS_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String TOPICS_GROUP =                     "Common";
  private static final int TOPICS_ORDER_IN_GROUP =               4;
  private static final ConfigDef.Width TOPICS_WIDTH =            ConfigDef.Width.LONG;
  private static final String TOPICS_DOC =
      "List of topics to consume, separated by commas";
  public static final String TOPICS_DEFAULT = "";
  private static final String TOPICS_DISPLAY =                   "Topics";

  public static final String TOPICS_REGEX_CONFIG =                     "topics.regex";
  private static final ConfigDef.Type TOPICS_REGEX_TYPE =              ConfigDef.Type.STRING;
  private static final ConfigDef.Importance TOPICS_REGEX_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String TOPICS_REGEX_GROUP =                     "Common";
  private static final int TOPICS_REGEX_ORDER_IN_GROUP =               4;
  private static final ConfigDef.Width TOPICS_REGEX_WIDTH =            ConfigDef.Width.LONG;
  private static final String TOPICS_REGEX_DOC = "Regular expression giving topics to consume. " +
	  "Under the hood, the regex is compiled to a <code>java.util.regex.Pattern</code>. " +
	  "Only one of " + TOPICS_CONFIG + " or " + TOPICS_REGEX_CONFIG + " should be specified.";
  public static final String TOPICS_REGEX_DEFAULT = "";
  private static final String TOPICS_REGEX_DISPLAY = "Topics regex";

  public static final String ENABLE_BATCH_CONFIG =                         "enableBatchLoad";
  private static final ConfigDef.Type ENABLE_BATCH_TYPE =                  ConfigDef.Type.LIST;
  private static final List<String> ENABLE_BATCH_DEFAULT =                 Collections.emptyList();
  private static final ConfigDef.Importance ENABLE_BATCH_IMPORTANCE =      ConfigDef.Importance.LOW;
  private static final String ENABLE_BATCH_DOC =
      "Beta Feature; use with caution: The sublist of topics to be batch loaded through GCS";

  public static final String BATCH_LOAD_INTERVAL_SEC_CONFIG =             "batchLoadIntervalSec";
  private static final ConfigDef.Type BATCH_LOAD_INTERVAL_SEC_TYPE =      ConfigDef.Type.INT;
  private static final Integer BATCH_LOAD_INTERVAL_SEC_DEFAULT =          120;
  private static final ConfigDef.Importance BATCH_LOAD_INTERVAL_SEC_IMPORTANCE =
      ConfigDef.Importance.LOW;
  private static final String BATCH_LOAD_INTERVAL_SEC_DOC =
      "The interval, in seconds, in which to attempt to run GCS to BQ load jobs. Only relevant "
      + "if enableBatchLoad is configured.";

  public static final String GCS_BUCKET_NAME_CONFIG =                     "gcsBucketName";
  private static final ConfigDef.Type GCS_BUCKET_NAME_TYPE =              ConfigDef.Type.STRING;
  private static final Object GCS_BUCKET_NAME_DEFAULT =                   "";
  private static final ConfigDef.Importance GCS_BUCKET_NAME_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String GCS_BUCKET_NAME_DOC =
      "The name of the bucket in which gcs blobs used to batch load to BigQuery "
      + "should be located. Only relevant if enableBatchLoad is configured.";

  public static final String GCS_FOLDER_NAME_CONFIG =                     "gcsFolderName";
  private static final ConfigDef.Type GCS_FOLDER_NAME_TYPE =              ConfigDef.Type.STRING;
  private static final Object GCS_FOLDER_NAME_DEFAULT =                   "";
  private static final ConfigDef.Importance GCS_FOLDER_NAME_IMPORTANCE =  ConfigDef.Importance.MEDIUM;
  private static final String GCS_FOLDER_NAME_DOC =
          "The name of the folder under the bucket in which gcs blobs used to batch load to BigQuery "
                  + "should be located. Only relevant if enableBatchLoad is configured.";

  public static final String TOPICS_TO_TABLES_CONFIG =                     "topicsToTables";
  private static final ConfigDef.Type TOPICS_TO_TABLES_TYPE =              ConfigDef.Type.LIST;
  private static final ConfigDef.Importance TOPICS_TO_TABLES_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  public static final Object TOPICS_TO_TABLES_DEFAULT =                    null;
  private static final String TOPICS_TO_TABLES_DOC =
      "A list of mappings from topic regexes to table names. Note the regex must include "
      + "capture groups that are referenced in the format string using placeholders (i.e. $1) "
      + "(form of <topic regex>=<format string>)";

  public static final String PROJECT_CONFIG =                     "project";
  private static final ConfigDef.Type PROJECT_TYPE =              ConfigDef.Type.STRING;
  private static final ConfigDef.Importance PROJECT_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String PROJECT_DOC =
      "The BigQuery project to write to";

  public static final String DATASETS_CONFIG =                     "datasets";
  private static final ConfigDef.Type DATASETS_TYPE =              ConfigDef.Type.LIST;
  private static final Object DATASETS_DEFAULT =                   ConfigDef.NO_DEFAULT_VALUE;
  private static final ConfigDef.Validator DATASETS_VALIDATOR =    validator;
  private static final ConfigDef.Importance DATASETS_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String DATASETS_DOC =
      "Names for the datasets kafka topics will write to "
      + "(form of <topic regex>=<dataset>)";

  public static final String SCHEMA_RETRIEVER_CONFIG =         "schemaRetriever";
  private static final ConfigDef.Type SCHEMA_RETRIEVER_TYPE =  ConfigDef.Type.CLASS;
  private static final Class<?> SCHEMA_RETRIEVER_DEFAULT =     null;
  private static final ConfigDef.Importance SCHEMA_RETRIEVER_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String SCHEMA_RETRIEVER_DOC =
      "A class that can be used for automatically creating tables and/or updating schemas";

  public static final String KEYFILE_CONFIG =                     "keyfile";
  private static final ConfigDef.Type KEYFILE_TYPE =              ConfigDef.Type.PASSWORD;
  public static final String KEYFILE_DEFAULT =                    null;
  private static final ConfigDef.Importance KEYFILE_IMPORTANCE =  ConfigDef.Importance.MEDIUM;
  private static final String KEYFILE_DOC =
      "The file containing a JSON key with BigQuery service account credentials";

  public static final String KEY_SOURCE_CONFIG =                "keySource";
  private static final ConfigDef.Type KEY_SOURCE_TYPE =         ConfigDef.Type.STRING;
  public static final String KEY_SOURCE_DEFAULT =               "FILE";
  private static final ConfigDef.Validator KEY_SOURCE_VALIDATOR =
          ConfigDef.ValidString.in("FILE", "JSON");
  private static final ConfigDef.Importance KEY_SOURCE_IMPORTANCE = ConfigDef.Importance.MEDIUM;
  private static final String KEY_SOURCE_DOC =
          "Determines whether the keyfile config is the path to the credentials json, or the json itself";

  public static final String SANITIZE_TOPICS_CONFIG =                     "sanitizeTopics";
  private static final ConfigDef.Type SANITIZE_TOPICS_TYPE =              ConfigDef.Type.BOOLEAN;
  public static final Boolean SANITIZE_TOPICS_DEFAULT =                   false;
  private static final ConfigDef.Importance SANITIZE_TOPICS_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String SANITIZE_TOPICS_DOC =
      "Whether to automatically sanitize topic names before using them as table names;"
      + " if not enabled topic names will be used directly as table names";

  public static final String SANITIZE_FIELD_NAME_CONFIG =                     "sanitizeFieldNames";
  private static final ConfigDef.Type SANITIZE_FIELD_NAME_TYPE =              ConfigDef.Type.BOOLEAN;
  public static final Boolean SANITIZE_FIELD_NAME_DEFAULT =                   false;
  private static final ConfigDef.Importance SANITIZE_FIELD_NAME_IMPORTANCE =
          ConfigDef.Importance.MEDIUM;
  private static final String SANITIZE_FIELD_NAME_DOC =
          "Whether to automatically sanitize field names before using them as field names in big query. "
                  + "Big query specifies that field name can only contain letters, numbers, and "
                  + "underscores. The sanitizer will replace the invalid symbols with underscore. "
                  + "If the field name starts with a digit, the sanitizer will add an underscore in "
                  + "front of field name. Note: field a.b and a_b will have same value after sanitizing, "
                  + "and might cause key duplication error.";

  public static final String KAFKA_KEY_FIELD_NAME_CONFIG =        "kafkaKeyFieldName";
  private static final ConfigDef.Type KAFKA_KEY_FIELD_NAME_TYPE = ConfigDef.Type.STRING;
  public static final String KAFKA_KEY_FIELD_NAME_DEFAULT =       null;
  private static final ConfigDef.Importance KAFKA_KEY_FIELD_NAME_IMPORTANCE = ConfigDef.Importance.LOW;
  private static final String KAFKA_KEY_FIELD_NAME_DOC = "The name of the field of Kafka key. " +
          "Default to be null, which means Kafka Key Field will not be included.";

  public static final String KAFKA_DATA_FIELD_NAME_CONFIG =        "kafkaDataFieldName";
  private static final ConfigDef.Type KAFKA_DATA_FIELD_NAME_TYPE = ConfigDef.Type.STRING;
  public static final String KAFKA_DATA_FIELD_NAME_DEFAULT =       null;
  private static final ConfigDef.Importance KAFKA_DATA_FIELD_NAME_IMPORTANCE = ConfigDef.Importance.LOW;
  private static final String KAFKA_DATA_FIELD_NAME_DOC = "The name of the field of Kafka Data. " +
          "Default to be null, which means Kafka Data Field will not be included. ";

  public static final String AVRO_DATA_CACHE_SIZE_CONFIG =                 "avroDataCacheSize";
  private static final ConfigDef.Type AVRO_DATA_CACHE_SIZE_TYPE =          ConfigDef.Type.INT;
  public static final Integer AVRO_DATA_CACHE_SIZE_DEFAULT =               100;
  private static final ConfigDef.Validator AVRO_DATA_CACHE_SIZE_VALIDATOR =
      ConfigDef.Range.atLeast(0);
  private static final ConfigDef.Importance AVRO_DATA_CACHE_SIZE_IMPORTANCE =
      ConfigDef.Importance.LOW;
  private static final String AVRO_DATA_CACHE_SIZE_DOC =
      "The size of the cache to use when converting schemas from Avro to Kafka Connect";

  public static final String CONVERT_DOUBLE_SPECIAL_VALUES_CONFIG =    "convertDoubleSpecialValues";
  public static final ConfigDef.Type CONVERT_DOUBLE_SPECIAL_VALUES_TYPE =   ConfigDef.Type.BOOLEAN;
  public static final Boolean CONVERT_DOUBLE_SPECIAL_VALUES_DEFAULT =       false;
  public static final ConfigDef.Importance CONVERT_DOUBLE_SPECIAL_VALUES_IMPORTANCE =
      ConfigDef.Importance.LOW;
  public static final String CONVERT_DOUBLE_SPECIAL_VALUES_DOC =
          "Should +Infinity be converted to Double.MAX_VALUE and -Infinity and NaN be "
          + "converted to Double.MIN_VALUE so they can make it to BigQuery";

  public static final String ALL_BQ_FIELDS_NULLABLE_CONFIG = "allBQFieldsNullable";
  private static final ConfigDef.Type ALL_BQ_FIELDS_NULLABLE_TYPE = ConfigDef.Type.BOOLEAN;
  private static final Boolean ALL_BQ_FIELDS_NULLABLE_DEFAULT = false;
  private static final ConfigDef.Importance ALL_BQ_FIELDS_NULLABLE_IMPORTANCE =
      ConfigDef.Importance.LOW;
  private static final String ALL_BQ_FIELDS_NULLABLE_DOC =
      "If true, no fields in any produced BigQuery schema will be REQUIRED. All "
      + "non-nullable avro fields will be translated as NULLABLE (or REPEATED, if arrays).";

  public static final String TABLE_CREATE_CONFIG =                     "autoCreateTables";
  private static final ConfigDef.Type TABLE_CREATE_TYPE =              ConfigDef.Type.BOOLEAN;
  public static final boolean TABLE_CREATE_DEFAULT =                   true;
  private static final ConfigDef.Importance TABLE_CREATE_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String TABLE_CREATE_DOC =
          "Automatically create BigQuery tables if they don't already exist";

  public static final String BEST_EFFORT_DEDUPLICATION_CONFIG =         "bestEffortDeduplication";
  private static final ConfigDef.Type BEST_EFFORT_DEDUPLICATION_TYPE =  ConfigDef.Type.BOOLEAN;
  public static final Boolean BEST_EFFORT_DEDUPLICATION_DEFAULT =       true;
  private static final ConfigDef.Importance BEST_EFFORT_DEDUPLICATION_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String BEST_EFFORT_DEDUPLICATION_DOC =
      "If false, Big Query best effort de-duplication will be disabled, which increases "
      + "the streaming ingest quota, at the expense of not checking for duplicates";

  static {
    config = new ConfigDef()
        .define(
            TOPICS_CONFIG,
            TOPICS_TYPE,
            TOPICS_DEFAULT,
            TOPICS_IMPORTANCE,
            TOPICS_DOC,
            TOPICS_GROUP,
            TOPICS_ORDER_IN_GROUP,
            TOPICS_WIDTH,
            TOPICS_DISPLAY)
        .define(
            TOPICS_REGEX_CONFIG,
            TOPICS_REGEX_TYPE,
            TOPICS_REGEX_DEFAULT,
            TOPICS_REGEX_IMPORTANCE,
            TOPICS_REGEX_DOC,
            TOPICS_REGEX_GROUP,
            TOPICS_REGEX_ORDER_IN_GROUP,
            TOPICS_REGEX_WIDTH,
            TOPICS_REGEX_DISPLAY)
        .define(
            ENABLE_BATCH_CONFIG,
            ENABLE_BATCH_TYPE,
            ENABLE_BATCH_DEFAULT,
            ENABLE_BATCH_IMPORTANCE,
            ENABLE_BATCH_DOC
        ).define(
            BATCH_LOAD_INTERVAL_SEC_CONFIG,
            BATCH_LOAD_INTERVAL_SEC_TYPE,
            BATCH_LOAD_INTERVAL_SEC_DEFAULT,
            BATCH_LOAD_INTERVAL_SEC_IMPORTANCE,
            BATCH_LOAD_INTERVAL_SEC_DOC
        ).define(
            GCS_BUCKET_NAME_CONFIG,
            GCS_BUCKET_NAME_TYPE,
            GCS_BUCKET_NAME_DEFAULT,
            GCS_BUCKET_NAME_IMPORTANCE,
            GCS_BUCKET_NAME_DOC
        ).define(
            GCS_FOLDER_NAME_CONFIG,
            GCS_FOLDER_NAME_TYPE,
            GCS_FOLDER_NAME_DEFAULT,
            GCS_FOLDER_NAME_IMPORTANCE,
            GCS_FOLDER_NAME_DOC
        ).define(
            TOPICS_TO_TABLES_CONFIG,
            TOPICS_TO_TABLES_TYPE,
            TOPICS_TO_TABLES_DEFAULT,
            TOPICS_TO_TABLES_IMPORTANCE,
            TOPICS_TO_TABLES_DOC
        ).define(
            PROJECT_CONFIG,
            PROJECT_TYPE,
            PROJECT_IMPORTANCE,
            PROJECT_DOC
        ).define(
            DATASETS_CONFIG,
            DATASETS_TYPE,
            DATASETS_DEFAULT,
            DATASETS_VALIDATOR,
            DATASETS_IMPORTANCE,
            DATASETS_DOC
        ).define(
            SCHEMA_RETRIEVER_CONFIG,
            SCHEMA_RETRIEVER_TYPE,
            SCHEMA_RETRIEVER_DEFAULT,
            SCHEMA_RETRIEVER_IMPORTANCE,
            SCHEMA_RETRIEVER_DOC
        ).define(
            KEYFILE_CONFIG,
            KEYFILE_TYPE,
            KEYFILE_DEFAULT,
            KEYFILE_IMPORTANCE,
            KEYFILE_DOC
        ).define(
            KEY_SOURCE_CONFIG,
            KEY_SOURCE_TYPE,
            KEY_SOURCE_DEFAULT,
            KEY_SOURCE_VALIDATOR,
            KEY_SOURCE_IMPORTANCE,
            KEY_SOURCE_DOC
        ).define(
            SANITIZE_TOPICS_CONFIG,
            SANITIZE_TOPICS_TYPE,
            SANITIZE_TOPICS_DEFAULT,
            SANITIZE_TOPICS_IMPORTANCE,
            SANITIZE_TOPICS_DOC
        ).define(
            SANITIZE_FIELD_NAME_CONFIG,
            SANITIZE_FIELD_NAME_TYPE,
            SANITIZE_FIELD_NAME_DEFAULT,
            SANITIZE_FIELD_NAME_IMPORTANCE,
            SANITIZE_FIELD_NAME_DOC
        ).define(
            KAFKA_KEY_FIELD_NAME_CONFIG,
            KAFKA_KEY_FIELD_NAME_TYPE,
            KAFKA_KEY_FIELD_NAME_DEFAULT,
            KAFKA_KEY_FIELD_NAME_IMPORTANCE,
            KAFKA_KEY_FIELD_NAME_DOC
        ).define(
            KAFKA_DATA_FIELD_NAME_CONFIG,
            KAFKA_DATA_FIELD_NAME_TYPE,
            KAFKA_DATA_FIELD_NAME_DEFAULT,
            KAFKA_DATA_FIELD_NAME_IMPORTANCE,
            KAFKA_DATA_FIELD_NAME_DOC
        ).define(
            AVRO_DATA_CACHE_SIZE_CONFIG,
            AVRO_DATA_CACHE_SIZE_TYPE,
            AVRO_DATA_CACHE_SIZE_DEFAULT,
            AVRO_DATA_CACHE_SIZE_VALIDATOR,
            AVRO_DATA_CACHE_SIZE_IMPORTANCE,
            AVRO_DATA_CACHE_SIZE_DOC
        ).define(
            ALL_BQ_FIELDS_NULLABLE_CONFIG,
            ALL_BQ_FIELDS_NULLABLE_TYPE,
            ALL_BQ_FIELDS_NULLABLE_DEFAULT,
            ALL_BQ_FIELDS_NULLABLE_IMPORTANCE,
            ALL_BQ_FIELDS_NULLABLE_DOC
        ).define(
            CONVERT_DOUBLE_SPECIAL_VALUES_CONFIG,
            CONVERT_DOUBLE_SPECIAL_VALUES_TYPE,
            CONVERT_DOUBLE_SPECIAL_VALUES_DEFAULT,
            CONVERT_DOUBLE_SPECIAL_VALUES_IMPORTANCE,
            CONVERT_DOUBLE_SPECIAL_VALUES_DOC
         ).define(
            TABLE_CREATE_CONFIG,
            TABLE_CREATE_TYPE,
            TABLE_CREATE_DEFAULT,
            TABLE_CREATE_IMPORTANCE,
            TABLE_CREATE_DOC
         ).define(
            BEST_EFFORT_DEDUPLICATION_CONFIG,
            BEST_EFFORT_DEDUPLICATION_TYPE,
            BEST_EFFORT_DEDUPLICATION_DEFAULT,
            BEST_EFFORT_DEDUPLICATION_IMPORTANCE,
            BEST_EFFORT_DEDUPLICATION_DOC
         );
  }
    /**
     * Throw an exception if the passed-in properties do not constitute a valid sink.
     * @param props sink configuration properties
     */
    public static void validate(Map<String, String> props) {
        final boolean hasTopicsConfig = hasTopicsConfig(props);
        final boolean hasTopicsRegexConfig = hasTopicsRegexConfig(props);

        if (hasTopicsConfig && hasTopicsRegexConfig) {
            throw new ConfigException(TOPICS_CONFIG + " and " + TOPICS_REGEX_CONFIG +
                " are mutually exclusive options, but both are set.");
        }

        if (!hasTopicsConfig && !hasTopicsRegexConfig) {
            throw new ConfigException("Must configure one of " +
                TOPICS_CONFIG + " or " + TOPICS_REGEX_CONFIG);
        }
    }

    public static boolean hasTopicsConfig(Map<String, String> props) {
        String topicsStr = props.get(TOPICS_CONFIG);
        return topicsStr != null && !topicsStr.trim().isEmpty();
    }

    public static boolean hasTopicsRegexConfig(Map<String, String> props) {
        String topicsRegexStr = props.get(TOPICS_REGEX_CONFIG);
        return topicsRegexStr != null && !topicsRegexStr.trim().isEmpty();
    }

  @SuppressWarnings("unchecked")
  public static class Validator implements ConfigDef.Validator {
    @Override
    public void ensureValid(String name, Object value) {
      switch (name) {
        case DATASETS_CONFIG:
          ensureValidMap(name, (List<String>) value);
          break;
        case TOPICS_TO_TABLES_CONFIG:
          ensureValidMap(name, (List<String>) value);
          break;
        default:
          break;
      }
    }

    protected static void ensureValidMap(String name, List<String> values) {
      if (values == null) {
        return;
      }
      values.forEach((entry) -> parseMapping(entry, name));
    }

    /**
    * Ensures the mapping given is valid, then returns an entry containing its key and value.
    * Checks to make sure that the given String adheres to the specified format, and throws
    * an exception if it does not. Trims leading and trailing whitespace, and then checks to make
    * sure that both Strings are still non-empty.
    *
    * @param mapping The mapping to parse (should be of the form &lt;key&gt;=&lt;value&gt;)
    * @param name The name of the field. Used in error messages.
    * @return A Map.Entry containing the parsed key/value pair.
    */
    protected static Map.Entry<String, String> parseMapping(String mapping, String name) {
      String[] keyValue = mapping.split("=");
      if (keyValue.length != 2) {
        throw new ConfigException(
            "Invalid mapping for " + name
            + " property: '" + mapping
            + "' (must follow format '<key>=<value>')"
        );
      }

      String key = keyValue[0].trim();
      if (key.isEmpty()) {
        throw new ConfigException(
            "Empty key found in mapping '" + mapping
            + "' for " + name + " property"
        );
      }

      String value = keyValue[1].trim();
      if (value.isEmpty()) {
        throw new ConfigException(
            "Empty value found in mapping '" + mapping
            + "' for " + name + " property"
        );
      }

      return new AbstractMap.SimpleEntry<>(key, value);
    }
  }

  /**
   * Returns the keyfile
   */
  public String getKeyFile() {
    return Optional.ofNullable(getPassword(KEYFILE_CONFIG)).map(Password::value).orElse(null);
  }

  /**
   * Parses a config map, which must be provided as a list of Strings of the form
   * '&lt;key&gt;=&lt;value&gt;' into a Map. Locates that list, splits its key and value pairs, and
   * returns they Map they represent.
   *
   * @param name The name of the property the mapping is given for. Used in exception messages.
   * @return A Map containing the given key and value pairs.
   */
  public Map<String, String> getMap(String name) {
    List<String> assocList = getList(name);
    Map<String, String> configMap = new HashMap<>();
    if (assocList != null) {
      for (String mapping : assocList) {
        Map.Entry<String, String> entry = validator.parseMapping(mapping, name);
        configMap.put(entry.getKey(), entry.getValue());
      }
    }
    return configMap;
  }

  /**
   * Given a config property that contains a list of [regex]=[string] mappings, returns a map from
   * the regex patterns to the strings.
   *
   * @param property The config name containing regex pattern key/value pairs.
   * @return A map of regex patterns to strings.
   */
  public List<Map.Entry<Pattern, String>> getSinglePatterns(String property) {
    List<String> propList = getList(property);
    List<Map.Entry<Pattern, String>> patternList = new ArrayList<>();
    if (propList != null) {
      for (String propValue : propList) {
        Map.Entry<String, String> mapping = validator.parseMapping(propValue, property);
        Pattern propPattern = Pattern.compile(mapping.getKey());
        Map.Entry<Pattern, String> patternEntry =
            new AbstractMap.SimpleEntry<>(propPattern, mapping.getValue());
        patternList.add(patternEntry);
      }
    }
    return patternList;
  }

  private Map<String, String> getSingleMatches(
      List<Map.Entry<Pattern, String>> patterns,
      List<String> values,
      String valueProperty,
      String patternProperty) {
    Map<String, String> matches = new HashMap<>();
    for (String value : values) {
      String match = null;
      for (Map.Entry<Pattern, String> pattern : patterns) {
        Matcher patternMatcher = pattern.getKey().matcher(value);
        if (patternMatcher.matches()) {
          if (match != null) {
            String secondMatch = pattern.getValue();
            throw new ConfigException(
                "Value '" + value
                + "' for property '" + valueProperty
                + "' matches " + patternProperty
                + " regexes for both '" + match
                + "' and '" + secondMatch + "'"
            );
          }
          match = pattern.getValue();
        }
      }
      if (match == null) {
        throw new ConfigException(
            "Value '" + value
            + "' for property '" + valueProperty
            + "' failed to match any of the provided " + patternProperty
            + " regexes"
        );
      }
      matches.put(value, match);
    }
    return matches;
  }

  /**
   * Return a String detailing which BigQuery dataset topic should write to.
   *
   * @param topicName The name of the topic for which dataset needs to be fetched.
   * @return A String associating Kafka topic name to BigQuery dataset.
   */
  public String getTopicToDataset(String topicName) {
    // Do not check for missing key in map as default empty map shall be returned.
    return getSingleMatches(
        getSinglePatterns(DATASETS_CONFIG),
        Collections.singletonList(topicName),
        TOPICS_CONFIG,
        DATASETS_CONFIG
    ).get(topicName);
  }


  /**
   * Return a Map detailing which BigQuery dataset each topic should write to.
   *
   * @return A Map associating Kafka topic names to BigQuery dataset.
   */
  public Map<String, String> getTopicsToDatasets() {
    return getSingleMatches(
        getSinglePatterns(DATASETS_CONFIG),
        getList(TOPICS_CONFIG),
        TOPICS_CONFIG,
        DATASETS_CONFIG
    );
  }

  /**
   * Return a new instance of the configured Schema Converter.
   * @return a {@link SchemaConverter} for BigQuery.
   */
  public SchemaConverter<Schema> getSchemaConverter() {
    return new BigQuerySchemaConverter(getBoolean(ALL_BQ_FIELDS_NULLABLE_CONFIG));
  }

  /**
   * Return a new instance of the configured Record Converter.
   * @return a {@link RecordConverter} for BigQuery.
   */
  public RecordConverter<Map<String, Object>> getRecordConverter() {
    return new BigQueryRecordConverter(getBoolean(CONVERT_DOUBLE_SPECIAL_VALUES_CONFIG));
  }

  /**
   * Locate the class specified by the user for use in retrieving schemas, and instantiate it.
   * @return A new instance of the user's specified SchemaRetriever class.
   * @throws ConfigException If the user did not specify a SchemaRetriever class.
   * @throws ConfigException If the specified class does not implement the SchemaRetriever
   *                         interface.
   * @throws ConfigException If the specified class does not have a no-args constructor.
   */
  public SchemaRetriever getSchemaRetriever() {
    Class<?> userSpecifiedClass = getClass(SCHEMA_RETRIEVER_CONFIG);

    if (userSpecifiedClass == null) {
      throw new ConfigException(
          "Cannot request new instance of SchemaRetriever when class has not been specified"
      );
    }

    if (!SchemaRetriever.class.isAssignableFrom(userSpecifiedClass)) {
      throw new ConfigException(
          "Class specified for " + SCHEMA_RETRIEVER_CONFIG
              + " property does not implement " + SchemaRetriever.class.getName()
              + " interface"
      );
    }

    Class<? extends SchemaRetriever> schemaRetrieverClass =
        userSpecifiedClass.asSubclass(SchemaRetriever.class);

    Constructor<? extends SchemaRetriever> schemaRetrieverConstructor = null;
    try {
      schemaRetrieverConstructor = schemaRetrieverClass.getConstructor();
    } catch (NoSuchMethodException nsme) {
      throw new ConfigException(
          "Class specified for SchemaRetriever must have a no-args constructor",
          nsme
      );
    }

    SchemaRetriever schemaRetriever = null;
    try {
      schemaRetriever = schemaRetrieverConstructor.newInstance();
    } catch (InstantiationException
        | IllegalAccessException
        | InvocationTargetException
        exception) {
      throw new ConfigException(
          "Failed to instantiate class specified for SchemaRetriever",
          exception
      );
    }

    schemaRetriever.configure(originalsStrings());

    return schemaRetriever;
  }

  /**
   *
   * If the connector is configured to load Kafka data into BigQuery, this config defines
   * the name of the kafka data field. A structure is created under the field name to contain
   * kafka data schema including topic, offset, partition and insertTime.
   *
   * @return Field name of Kafka Data to be used in BigQuery
   */
  public Optional<String> getKafkaKeyFieldName() {
    return Optional.ofNullable(getString(KAFKA_KEY_FIELD_NAME_CONFIG));
  }

  /**
   *
   * If the connector is configured to load Kafka keys into BigQuery, this config defines
   * the name of the kafka key field. A structure is created under the field name to contain
   * a topic's Kafka key schema.
   *
   * @return Field name of Kafka Key to be used in BigQuery
   */
  public Optional<String> getKafkaDataFieldName() {
    return Optional.ofNullable(getString(KAFKA_DATA_FIELD_NAME_CONFIG));
  }

  /**
   * Verifies that a bucket is specified if GCS batch loading is enabled.
   * @throws ConfigException Exception thrown if no bucket is specified and batch loading is on.
   */
  private void verifyBucketSpecified() throws ConfigException {
    // Throw an exception if GCS Batch loading will be used but no bucket is specified
    if (getString(GCS_BUCKET_NAME_CONFIG).equals("")
        && !getList(ENABLE_BATCH_CONFIG).isEmpty()) {
      throw new ConfigException("Batch loading enabled for some topics, but no bucket specified");
    }
  }

  private void checkAutoCreateTables() {

    Class<?> schemaRetriever = getClass(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG);
    boolean autoCreateTables = getBoolean(TABLE_CREATE_CONFIG);

    if (autoCreateTables && schemaRetriever == null) {
      throw new ConfigException(
        "Cannot specify automatic table creation without a schema retriever"
      );
    }
  }

  /**
   * Return the ConfigDef object used to define this config's fields.
   *
   * @return The ConfigDef object used to define this config's fields.
   */
  public static ConfigDef getConfig() {
    return config;
  }

  protected BigQuerySinkConfig(ConfigDef config, Map<String, String> properties) {
    super(config, properties);
    verifyBucketSpecified();
  }

  public BigQuerySinkConfig(Map<String, String> properties) {
    super(config, properties);
    verifyBucketSpecified();
    checkAutoCreateTables();
  }

}
