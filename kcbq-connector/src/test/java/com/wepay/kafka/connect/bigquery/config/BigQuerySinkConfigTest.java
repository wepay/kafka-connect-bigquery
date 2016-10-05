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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.SinkPropertiesFactory;

import com.wepay.kafka.connect.bigquery.convert.kafkadata.KafkaDataBQRecordConverter;
import com.wepay.kafka.connect.bigquery.convert.kafkadata.KafkaDataBQSchemaConverter;
import org.apache.kafka.common.config.ConfigException;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BigQuerySinkConfigTest {
  private SinkPropertiesFactory propertiesFactory;

  @Before
  public void initializePropertiesFactory() {
    propertiesFactory = new SinkPropertiesFactory();
  }

  // Just to ensure that the basic properties don't cause any exceptions on any public methods
  @Test
  public void metaTestBasicConfigProperties() {
    Map<String, String> basicConfigProperties = propertiesFactory.getProperties();
    BigQuerySinkConfig config = new BigQuerySinkConfig(basicConfigProperties);
    propertiesFactory.testProperties(config);
  }

  @Test
  public void testTopicsToDatasets() {
    Map<String, String> configProperties = propertiesFactory.getProperties();

    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        "(tracking-.*)|(.*-tracking)=tracking,.*event.*=events"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "tracking-login,clicks-tracking,db-event,misc-event_stuff"
    );

    Map<String, String> expectedTopicsToDatasets = new HashMap<>();
    expectedTopicsToDatasets.put("tracking-login", "tracking");
    expectedTopicsToDatasets.put("clicks-tracking", "tracking");
    expectedTopicsToDatasets.put("db-event", "events");
    expectedTopicsToDatasets.put("misc-event_stuff", "events");

    Map<String, String> testTopicsToDatasets =
        new BigQuerySinkConfig(configProperties).getTopicsToDatasets();

    assertEquals(expectedTopicsToDatasets, testTopicsToDatasets);
  }

  @Test
  public void testTablesToTopics() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "sanitize-me,leave_me_alone"
    );

    Map<TableId, String> expectedTablesToSchemas = new HashMap<>();
    expectedTablesToSchemas.put(TableId.of("scratch", "sanitize_me"), "sanitize-me");
    expectedTablesToSchemas.put(TableId.of("scratch", "leave_me_alone"), "leave_me_alone");

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    Map<String, String> topicsToDatasets = testConfig.getTopicsToDatasets();
    Map<TableId, String> testTablesToSchemas = testConfig.getTablesToTopics(topicsToDatasets);

    assertEquals(expectedTablesToSchemas, testTablesToSchemas);
  }

  @Test
  public void testTopicsToTables() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "sanitize-me,db_debezium_identity_profiles_info"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        "db_debezium_identity_profiles_(.*)=$1,(.*)-me=$1_myself"
    );
    Map<TableId, String> expectedTablesToSchemas = new HashMap<>();
    expectedTablesToSchemas.put(TableId.of("scratch", "sanitize_myself"), "sanitize-me");
    expectedTablesToSchemas.put(TableId.of("scratch", "info"), "db_debezium_identity_profiles_info");

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    Map<String, String> topicsToDatasets = testConfig.getTopicsToDatasets();
    Map<TableId, String> testTablesToSchemas = testConfig.getTablesToTopics(topicsToDatasets);

    assertEquals(expectedTablesToSchemas, testTablesToSchemas);
  }

  @Test(expected=ConfigException.class)
  public void testInvalidTopicsToTables() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.SANITIZE_TOPICS_CONFIG, "true");
    configProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        ".*=scratch"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "sanitize-me,db_debezium_identity_profiles_info"
    );
    configProperties.put(
        BigQuerySinkConfig.TOPICS_TO_TABLES_CONFIG,
        ".*=$1"
    );
    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);
    Map<String, String> topicsToDatasets = testConfig.getTopicsToDatasets();
    testConfig.getTablesToTopics(topicsToDatasets);
  }

  @Test
  public void testGetSchemaConverter() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.INCLUDE_KAFKA_DATA_CONFIG, "true");

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);

    assertTrue(testConfig.getSchemaConverter() instanceof KafkaDataBQSchemaConverter);
  }

  @Test
  public void testGetRecordConverter() {
    Map<String, String> configProperties = propertiesFactory.getProperties();
    configProperties.put(BigQuerySinkConfig.INCLUDE_KAFKA_DATA_CONFIG, "true");

    BigQuerySinkConfig testConfig = new BigQuerySinkConfig(configProperties);

    assertTrue(testConfig.getRecordConverter() instanceof KafkaDataBQRecordConverter);
  }

  @Test(expected = ConfigException.class)
  public void testFailedDatasetMatch() {
    Map<String, String> badConfigProperties = propertiesFactory.getProperties();

    badConfigProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        "[^q]+=kafka_connector_test"
    );
    badConfigProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "abcdefghijklmnoprstuvwxyz, ABCDEFGHIJKLMNOPQRSTUVWXYZ, 0123456789_9876543210"
    );

    try {
      new BigQuerySinkConfig(badConfigProperties).getTopicsToDatasets();
    } catch (ConfigException err) {
      fail("Exception encountered before addition of bad configuration field: " + err);
    }

    badConfigProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "nq"
    );
    new BigQuerySinkConfig(badConfigProperties).getTopicsToDatasets();
  }

  @Test(expected = ConfigException.class)
  public void testMultipleDatasetMatches() {
    Map<String, String> badConfigProperties = propertiesFactory.getProperties();

    badConfigProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        "(?iu)tracking(?-iu)-.+=kafka_connector_test_tracking, "
        + ".+-(?iu)database=kafka_connector_test_database"
    );
    badConfigProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "tracking-clicks, TRACKING-links, TRacKinG-., sql-database, bigquery-DATABASE, --dataBASE"
    );

    try {
      new BigQuerySinkConfig(badConfigProperties).getTopicsToDatasets();
    } catch (ConfigException err) {
      fail("Exception encountered before addition of bad configuration field: " + err);
    }

    badConfigProperties.put(
        BigQuerySinkConfig.TOPICS_CONFIG,
        "tracking-everything-in-the-database"
    );
    new BigQuerySinkConfig(badConfigProperties).getTopicsToDatasets();
  }

  @Test(expected = ConfigException.class)
  public void testInvalidMappingLackingEquals() {
    Map<String, String> badConfigProperties = propertiesFactory.getProperties();

    badConfigProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        "this_is_not_a_mapping"
    );

    new BigQuerySinkConfig(badConfigProperties);
  }

  @Test(expected = ConfigException.class)
  public void testInvalidMappingHavingMultipleEquals() {
    Map<String, String> badConfigProperties = propertiesFactory.getProperties();

    badConfigProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        "this=is=not=a=mapping"
    );

    new BigQuerySinkConfig(badConfigProperties);
  }

  @Test(expected = ConfigException.class)
  public void testEmptyMapKey() {
    Map<String, String> badConfigProperties = propertiesFactory.getProperties();

    badConfigProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        "=empty_key"
    );

    new BigQuerySinkConfig(badConfigProperties);
  }

  @Test(expected = ConfigException.class)
  public void testEmptyMapValue() {
    Map<String, String> badConfigProperties = propertiesFactory.getProperties();

    badConfigProperties.put(
        BigQuerySinkConfig.DATASETS_CONFIG,
        "empty_value="
    );

    new BigQuerySinkConfig(badConfigProperties);
  }

  @Test(expected = ConfigException.class)
  public void testInvalidAvroCacheSize() {
    Map<String, String> badConfigProperties = propertiesFactory.getProperties();

    badConfigProperties.put(
        BigQuerySinkConfig.AVRO_DATA_CACHE_SIZE_CONFIG,
        "-1"
    );

    new BigQuerySinkConfig(badConfigProperties);
  }
}
