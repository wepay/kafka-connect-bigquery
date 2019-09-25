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
        match = value;
      }
      if (sanitize) {
        match = sanitizeTableName(match);
      }
      String dataset = topicsToDatasets.get(value);
      matches.put(value, TableId.of(dataset, match));
    }
    return matches;
  }

  /**
   * Return a Map detailing which topic each base table corresponds to. If sanitization has been
   * enabled, there is a possibility that there are multiple possible schemas a table could
   * correspond to. In that case, each table must only be written to by one topic, or an exception
   * is thrown.
   *
   * @param config Config that contains properties used to generate the map
   * @return The resulting Map from TableId to topic name.
   */
  public static Map<TableId, String> getBaseTablesToTopics(BigQuerySinkConfig config) {
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
