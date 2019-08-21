package com.wepay.kafka.connect.bigquery.utils;

import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import org.apache.kafka.common.config.ConfigException;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.wepay.kafka.connect.bigquery.utils.TopicToTableResolver.getTopicToTableSingleMatch;

public class TopicToTableUpdater {
  public static String getTopicToDateset (
          List<Map.Entry<Pattern, String>> patterns,
          String topicName,
          String valueProperty,
          String patternProperty) {

    String match = null;
    for (Map.Entry<Pattern, String> pattern : patterns) {
      Matcher patternMatcher = pattern.getKey().matcher(topicName);
      if (patternMatcher.matches()) {
        if (match != null) {
          String secondMatch = pattern.getValue();
          throw new ConfigException(
                  "Value '" + topicName
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
              "Value '" + topicName
                      + "' for property '" + valueProperty
                      + "' failed to match any of the provided " + patternProperty
                      + " regexes"
      );
    }

    return match;
  }

  /**
   * Return a Map detailing which BigQuery table each topic should write to.
   *
   * @param config Config that contains properties used to generate the map
   * @return A Map associating Kafka topic names to BigQuery table names.
   */
  public static Map<String, TableId> updateTopicToTable(
          BigQuerySinkConfig config,
          String topicName,
          Map<String, TableId> topicToTable) {
    Boolean sanitize = config.getBoolean(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG);
    String match = getTopicToTableSingleMatch(config, topicName);

    if (match == null) {
      match = topicName;
    }
    if (sanitize) {
      match = sanitizeTableName(match);
    }
    String dataset =
            getTopicToDateset(
                    config.getSinglePatterns(config.DATASETS_CONFIG),
                    topicName,
                    config.TOPICS_CONFIG,
                    config.DATASETS_CONFIG
            );
    topicToTable.put(topicName, TableId.of(dataset, match));
    return topicToTable;
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
