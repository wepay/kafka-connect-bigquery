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

import static org.junit.Assert.assertEquals;

import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.SinkPropertiesFactory;
import com.wepay.kafka.connect.bigquery.api.TopicAndRecordName;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TopicToTableResolverTest {

  private SinkPropertiesFactory propertiesFactory;

  @Before
  public void initializePropertiesFactory() {
    propertiesFactory = new SinkPropertiesFactory();
  }

  @Test
  public void testGetTopicsToTablesMap() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "sanitize-me,db_debezium_identity_profiles_info.foo,db.core.cluster-0.users"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "db_debezium_identity_profiles_(.*)=$1,db\\.(.*)\\.(.*)\\.(.*)=$1_$3"
    );
    Map<TopicAndRecordName, TableId> expectedTopicsToTables = new HashMap<>();
    expectedTopicsToTables.put(TopicAndRecordName.from("sanitize-me"), TableId.of("scratch", "sanitize_me"));
    expectedTopicsToTables.put(TopicAndRecordName.from("db_debezium_identity_profiles_info.foo"),
        TableId.of("scratch", "info_foo"));
    expectedTopicsToTables.put(TopicAndRecordName.from("db.core.cluster-0.users"), TableId.of("scratch", "core_users"));

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    Map<TopicAndRecordName, TableId> topicsToTableIds = TopicToTableResolver.getTopicsToTables(testConfig);

    assertEquals(expectedTopicsToTables, topicsToTableIds);
  }

  @Test
  public void testGetTopicsToTablesEmptyMap() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SUPPORT_MULTI_SCHEMA_TOPICS_CONFIG, "true");
    Map<TopicAndRecordName, TableId> expectedTopicsToTables = Collections.emptyMap();

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    Map<TopicAndRecordName, TableId> topicsToTableIds = TopicToTableResolver.getTopicsToTables(testConfig);

    assertEquals(expectedTopicsToTables, topicsToTableIds);
  }

  @Test(expected = ConfigException.class)
  public void testTopicsToTablesInvalidRegex() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "db_debezium_identity_profiles_info"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        ".*=$1"
    );
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.getTopicsToTables(testConfig);
  }

  @Test(expected = ConfigException.class)
  public void testTopicsToTablesMultipleMatches() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "db_debezium_identity_profiles_info"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "(.*)=$1,db_debezium_identity_profiles_(.*)=$1"
    );
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.getTopicsToTables(testConfig);
  }

  @Test
  public void testUpdateTopicToTable() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "sanitize-me,db_debezium_identity_profiles_info.foo,db.core.cluster-0.users"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "db_debezium_identity_profiles_(.*)=$1,db\\.(.*)\\.(.*)\\.(.*)=$1_$3"
    );
    Map<TopicAndRecordName, TableId> topicsToTables = new HashMap<>();
    topicsToTables.put(TopicAndRecordName.from("sanitize-me"), TableId.of("scratch", "sanitize_me"));
    topicsToTables.put(TopicAndRecordName.from("db_debezium_identity_profiles_info.foo"),
        TableId.of("scratch", "info_foo"));
    topicsToTables.put(TopicAndRecordName.from("db.core.cluster-0.users"), TableId.of("scratch", "core_users"));

    TopicAndRecordName testTopicAndRecordName = TopicAndRecordName.from("new_topic", "some.record.Name");
    // Create shallow copy of map, deep copy not needed.
    Map<TopicAndRecordName, TableId> expectedTopicsToTables = new HashMap<>(topicsToTables);
    expectedTopicsToTables.put(testTopicAndRecordName, TableId.of("scratch", testTopicAndRecordName.getTopic()));

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.updateTopicToTable(testConfig, testTopicAndRecordName, topicsToTables);

    assertEquals(expectedTopicsToTables, topicsToTables);
  }

  @Test
  public void testUpdateTopicToTableWithTableSanitization() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "db_debezium_identity_profiles_(.*)=$1,db\\.(.*)\\.(.*)\\.(.*)=$1_$3"
    );

    TopicAndRecordName testTopicAndRecordName = TopicAndRecordName.from("1new.topic", "some.record.Name");
    Map<TopicAndRecordName, TableId> topicsToTables = new HashMap<>();
    // Create shallow copy of map, deep copy not needed.
    Map<TopicAndRecordName, TableId> expectedTopicsToTables = new HashMap<>(topicsToTables);
    expectedTopicsToTables.put(testTopicAndRecordName, TableId.of("scratch", "_1new_topic"));

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.updateTopicToTable(testConfig, testTopicAndRecordName, topicsToTables);

    assertEquals(expectedTopicsToTables, topicsToTables);
  }

  @Test
  public void testUpdateTopicToTableWithRegex() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "db_debezium_identity_profiles_(.*)=$1,db\\.(.*)\\.(.*)\\.(.*)=$1_$3,new_topic_(.*)=$1"
    );

    TopicAndRecordName testTopicAndRecordName = TopicAndRecordName.from("new_topic_abc.def", "some.record.Name");
    Map<TopicAndRecordName, TableId> topicsToTables = new HashMap<>();
    // Create shallow copy of map, deep copy not needed.
    Map<TopicAndRecordName, TableId> expectedTopicsToTables = new HashMap<>(topicsToTables);
    expectedTopicsToTables.put(testTopicAndRecordName, TableId.of("scratch", "abc_def"));

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.updateTopicToTable(testConfig, testTopicAndRecordName, topicsToTables);

    assertEquals(expectedTopicsToTables, topicsToTables);
  }

  @Test
  public void testUpdateTopicToTableWithRegexAndMultiSchemaTopicsEnabled() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "db_debezium_identity_profiles_(.*)=$1,db\\.(.*)\\.(.*)\\.(.*)=$1_$3,new_topic_(.*)=$1"
    );
    configProperties.put(
        BigQuerySinkConfig.RECORD_ALIASES_CONFIG,
        "some\\.record\\.(.*)=$1"
    );
    configProperties.put(
        BigQuerySinkConfig.SUPPORT_MULTI_SCHEMA_TOPICS_CONFIG,
        "true"
    );

    TopicAndRecordName testTopicWithMatchingRecordName = TopicAndRecordName.from("new_topic_abc.def", "some.record.RecordName");
    TopicAndRecordName testTopicWithUnexpectedRecordName = TopicAndRecordName.from("new_topic_abc.def", "unexpected.RecordName");
    Map<TopicAndRecordName, TableId> topicsToTables = new HashMap<>();
    // Create shallow copy of map, deep copy not needed.
    Map<TopicAndRecordName, TableId> expectedTopicsToTables = new HashMap<>(topicsToTables);
    expectedTopicsToTables.put(testTopicWithMatchingRecordName, TableId.of("scratch", "abc_def_RecordName"));
    expectedTopicsToTables.put(testTopicWithUnexpectedRecordName, TableId.of("scratch", "abc_def_unexpected_RecordName"));

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.updateTopicToTable(testConfig, testTopicWithMatchingRecordName, topicsToTables);
    TopicToTableResolver.updateTopicToTable(testConfig, testTopicWithUnexpectedRecordName, topicsToTables);

    assertEquals(expectedTopicsToTables, topicsToTables);
  }

  @Test(expected = ConfigException.class)
  public void testUpdateTopicToTableWithInvalidRegex() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        ".*=$1"
    );

    TopicAndRecordName testTopicAndRecordName = TopicAndRecordName.from("new_topic_abc.def", "some.record.Name");
    Map<TopicAndRecordName, TableId> topicsToTables = new HashMap<>();

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.updateTopicToTable(testConfig, testTopicAndRecordName, topicsToTables);
  }

  @Test(expected = ConfigException.class)
  public void testUpdateTopicToTableWithMultipleMatches() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "(.*)=$1,new_topic_(.*)=$1"
    );

    TopicAndRecordName testTopicAndRecordName = TopicAndRecordName.from("new_topic_abc.def", "some.record.Name");
    Map<TopicAndRecordName, TableId> topicsToTables = new HashMap<>();

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.updateTopicToTable(testConfig, testTopicAndRecordName, topicsToTables);
  }

  @Test(expected = ConfigException.class)
  public void testUpdateTopicToTableWithMultipleRecordNameMatches() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "new_topic_(.*)=$1"
    );
    configProperties.put(
        BigQuerySinkConfig.RECORD_ALIASES_CONFIG,
        "(.*)=$1,some\\.record\\.(.*)=$1"
    );
    configProperties.put(
        BigQuerySinkConfig.SUPPORT_MULTI_SCHEMA_TOPICS_CONFIG,
        "true"
    );

    TopicAndRecordName testTopicAndRecordName = TopicAndRecordName.from("new_topic_abc.def", "some.record.Name");
    Map<TopicAndRecordName, TableId> topicsToTables = new HashMap<>();

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    TopicToTableResolver.updateTopicToTable(testConfig, testTopicAndRecordName, topicsToTables);
  }

}
