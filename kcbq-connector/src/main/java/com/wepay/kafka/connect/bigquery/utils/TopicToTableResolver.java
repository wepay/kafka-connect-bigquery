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
import com.google.common.collect.Maps;
import com.wepay.kafka.connect.bigquery.api.TopicAndRecordName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A utility class that will resolve topic names to table names based on format strings using regex
 * capture groups.
 */
public class TopicToTableResolver {

  /**
   * Return a Map detailing which BigQuery table each topic should write to.
   *
   * @param config Config that contains properties used to generate the map.
   * @return A Map associating Kafka topic names to BigQuery table names.
   */
  public static Map<TopicAndRecordName, TableId> getTopicsToTables(BigQuerySinkConfig config) {
    // Based only on the config we cannot determine what record type is used in which topic.
    // It will be eventually discovered by handling new messages.
    if (config.getBoolean(BigQuerySinkConfig.SUPPORT_MULTI_SCHEMA_TOPICS_CONFIG)) {
      return Maps.newHashMap();
    }

    Map<String, String> topicsToDatasets = config.getTopicsToDatasets();

    List<String> topics = config.getList(BigQuerySinkConfig.TOPICS_CONFIG);
    Boolean sanitize = config.getBoolean(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG);

    Map<TopicAndRecordName, TableId> matches = new HashMap<>();
    for (String value : topics) {
      String match = getTopicToTableSingleMatch(config, value);
      if (match == null) {
        match = value;
      }

      if (sanitize) {
        match = FieldNameSanitizer.sanitizeName(match);
      }

      String dataset = topicsToDatasets.get(value);
      matches.put(TopicAndRecordName.from(value), TableId.of(dataset, match));
    }

    return matches;
  }

  /**
   * Return a Map detailing which BigQuery table each topic should write to.
   *
   * @param config Config that contains properties used to generate the map.
   * @param schemasByTopic A Map containing data for topic to respective schema.
   * @return A Map associating Kafka topic names to BigQuery table names.
   */
  public static Map<TopicAndRecordName, TableId> getTopicsToTables(BigQuerySinkConfig config, Map<TopicAndRecordName, Schema> schemasByTopic) {
    Map<TopicAndRecordName, TableId> topicToTableIds = Maps.newHashMap();
    schemasByTopic.forEach((key, value) -> updateTopicToTable(config, key, topicToTableIds));
    return topicToTableIds;
  }

  /**
   * Update Map detailing BigQuery table for respective topic should write to.
   *
   * @param config Config that contains properties used to generate the map.
   * @param topicAndRecordName The name of respective topic and an optional record name to map with table.
   * @param topicToTable Map containing data for topic to respective table.
   */
  public static void updateTopicToTable(BigQuerySinkConfig config, TopicAndRecordName topicAndRecordName,
                                                      Map<TopicAndRecordName, TableId> topicToTable) {
    // Though the methods getTopicsToTable and updateTopicToTable are similar but code is not merged
    // as they slightly operate in different way. Former fetches complete topicsToDatasets map and
    // act on same while latter only fetches single match for dataset as required by topicName.
    Boolean sanitize = config.getBoolean(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG);
    Boolean supportMultiSchemaTopics = config.getBoolean(BigQuerySinkConfig.SUPPORT_MULTI_SCHEMA_TOPICS_CONFIG);
    String match = Optional.ofNullable(getTopicToTableSingleMatch(config, topicAndRecordName.getTopic()))
        .orElse(topicAndRecordName.getTopic());

    if (supportMultiSchemaTopics) {
      String recordName = topicAndRecordName.getRecordName()
          .map(rn -> getRecordToTableSingleMatch(config, rn))
          .map(recordMatch -> "_" + recordMatch)
          .orElse("");
      match = match + recordName;
    }

    if (sanitize) {
      match = FieldNameSanitizer.sanitizeName(match);
    }

    String dataset = config.getTopicToDataset(topicAndRecordName.getTopic());
    // Do not check for dataset being null as TableId construction shall take care of same in below
    // line.
    topicToTable.put(topicAndRecordName, TableId.of(dataset, match));
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
  public static Map<TableId, TopicAndRecordName> getBaseTablesToTopics(BigQuerySinkConfig config) {
    Map<TopicAndRecordName, TableId> topicsToTableIds = getTopicsToTables(config);
    Map<TableId, TopicAndRecordName> tableIdsToTopics = new HashMap<>();
    for (Map.Entry<TopicAndRecordName, TableId> topicToTableId : topicsToTableIds.entrySet()) {
      if (tableIdsToTopics.put(topicToTableId.getValue(), topicToTableId.getKey()) != null) {
        throw new ConfigException("Cannot have multiple topics writing to the same table");
      }
    }
    return tableIdsToTopics;
  }

  /**
   * Return a String specifying table corresponding to topic.
   *
   * @param config Config that contains properties for configured patterns.
   * @param topicName The name of topic for which match is to be found.
   * @return A String resulting match of table for topic name.
   */
  private static String getTopicToTableSingleMatch(BigQuerySinkConfig config, String topicName) {
    return config.getSingleMatch(topicName, BigQuerySinkConfig.TOPICS_CONFIG, BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG);
  }

  /**
   * Return a String specifying alias corresponding to record name.
   *
   * @param config     Config that contains properties for configured patterns.
   * @param recordName The record name for which match is to be found.
   * @return A String resulting match of alias for record name or just the record name if no matching alias was found.
   */
  private static String getRecordToTableSingleMatch(BigQuerySinkConfig config, String recordName) {
    return Optional.ofNullable(config.getSingleMatch(recordName, "record name", BigQuerySinkConfig.RECORD_ALIASES_CONFIG))
        .orElse(recordName);
  }

}
