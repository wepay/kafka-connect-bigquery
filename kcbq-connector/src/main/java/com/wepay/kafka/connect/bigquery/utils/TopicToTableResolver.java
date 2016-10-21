package com.wepay.kafka.connect.bigquery.utils;

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


import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import org.apache.kafka.common.config.ConfigException;

import java.time.Clock;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A utility class that will resolve topic names to table names based on format strings using regex
 * capture groups.
 */
public class TopicToTableResolver {

  private static final String PARTITION_DELIMITER = "$";
  private static final Clock UTC_CLOCK = Clock.systemUTC();

  /**
   * Return a Map detailing which BigQuery table each topic should write to.
   *
   * @param config Config that contains properties used to generate the map
   * @return A Map associating Kafka topic names to BigQuery table names.
   */
  public static Map<String, TableId> getTopicsToTables(BigQuerySinkConfig config) {
    Map<String, String> topicsToDatasets = config.getTopicsToDatasets();
    List<Map.Entry<Pattern, String>> patterns = config.getSinglePatterns(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG);
    List<String> topics = config.getList(BigQuerySinkConfig.TOPICS_CONFIG);
    Boolean sanitize = config.getBoolean(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG);
    Map<String, TableId> matches = new HashMap<>();
    for (String value : topics) {
      String match = null;
      String previousPattern = null;
      for (Map.Entry<Pattern, String> pattern : patterns) {
        Matcher patternMatcher = pattern.getKey().matcher(value);
        if (patternMatcher.matches()) {
          if (match != null) {
            String secondMatch = pattern.getKey().toString();
            throw new ConfigException("Value '" + value
              + "' for property '" + BigQuerySinkConfig.TOPICS_CONFIG
              + "' matches " + BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG
              + " regexes for both '" + previousPattern
              + "' and '" + secondMatch + "'"
            );
          }
          String formatString = pattern.getValue();
          try {
            match = patternMatcher.replaceAll(formatString);
            previousPattern = pattern.getKey().toString();
          } catch (IndexOutOfBoundsException err) {
            throw new ConfigException("Format string '" + formatString
              + "' is invalid in property '" + BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG
              + "'", err);
          }
        }
      }
      if (match == null) {
        match = (sanitize) ? sanitizeTableName(value) : value;
      }
      String dataset = topicsToDatasets.get(value);
      matches.put(value, TableId.of(dataset, match));
    }
    return matches;
  }

  // package private for testing
  static TableId getPartitionedTableName(TableId baseTableId, LocalDate localDate) {
    StringBuilder sb = new StringBuilder();
    String baseTableName = baseTableId.table();
    sb.append(baseTableName);
    sb.append(PARTITION_DELIMITER);

    int year = localDate.getYear();
    int month = localDate.getMonthValue();
    int day = localDate.getDayOfMonth();
    sb.append(year);
    sb.append(month);
    sb.append(day);
    String partitionedTableName = sb.toString();
    if (baseTableId.project() == null) {
      return TableId.of(baseTableId.dataset(), partitionedTableName);
    } else {
      return TableId.of(baseTableId.project(), baseTableId.dataset(), partitionedTableName);
    }
  }

  /**
   * Create and return a TableId containing partition data for the current UTC date.
   *
   * @param baseTableId The tableId with no partition info.
   * @return the tableId with the partition data for the current UTC date.
   */
  public static TableId getPartitionedTableName(TableId baseTableId) {
    return getPartitionedTableName(baseTableId, LocalDate.now(UTC_CLOCK));
  }

  /**
   * Create and return a TableId that does not contain any partition data.
   * If the given table is not partitioned, returns the given table without changes.
   *
   * @param partitionedTableId TableId with partition data.
   * @return a baseTableId
   */
  public static TableId getBaseTableName(TableId partitionedTableId) {
    String partitionedTableName = partitionedTableId.table();
    String[] splitTableName = partitionedTableName.split(PARTITION_DELIMITER);

    if (splitTableName.length == 1) {
      // table not partitioned
      return partitionedTableId;
    } else if (splitTableName.length == 2) {
      return createTableId(partitionedTableId.project(),
                           partitionedTableId.dataset(),
                           splitTableName[0]);
    } else {
      // something has gone horribly wrong.
      throw new IllegalArgumentException(
          "Attempted to get the base table name of '" + partitionedTableName
              + "', but that is not a legal table name.");
    }
  }

  /**
   * Convenience method for creating new TableIds.
   * <p>
   * {@link TableId#of(String, String, String)} will error if you pass in a null project, so this
   * just checks if the project is null and calls the correct method.
   *
   * @param project the project name, or null
   * @param dataset the dataset name
   * @param table the table name
   * @return a new TableId with the given project, dataset, and table.
   */
  private static TableId createTableId(String project, String dataset, String table) {
    if (project == null) {
      return TableId.of(dataset, table);
    } else {
      return TableId.of(project, dataset, table);
    }
  }

  /**
   * Return a Map detailing which topic each table corresponds to. If sanitization has been enabled,
   * there is a possibility that there are multiple possible schemas a table could correspond to. In
   * that case, each table must only be written to by one topic, or an exception is thrown.
   *
   * @param config Config that contains properties used to generate the map
   * @return The resulting Map from TableId to topic name.
   */
  public static Map<TableId, String> getTablesToTopics(BigQuerySinkConfig config) {
    Map<String, TableId> topicsToTableIds = getTopicsToTables(config);
    Map<TableId, String> tableIdsToTopics = new HashMap<>();
    for (Map.Entry<String, TableId> topicToTableId : topicsToTableIds.entrySet()) {
      if (tableIdsToTopics.put(topicToTableId.getValue(), topicToTableId.getKey()) != null) {
        throw new ConfigException("Cannot have multiple topics writing to the same table");
      }
    }
    return tableIdsToTopics;
  }

  /**
   * Strips illegal characters from a table name. BigQuery only allows alpha-numeric and
   * underscore. Everything illegal is converted to an underscore.
   *
   * @param tableName The table name to sanitize.
   * @return A clean table name with only alpha-numerics and underscores.
   */
  private static String sanitizeTableName(String tableName) {
    return tableName.replaceAll("[^a-zA-Z0-9_]", "_");
  }
}
