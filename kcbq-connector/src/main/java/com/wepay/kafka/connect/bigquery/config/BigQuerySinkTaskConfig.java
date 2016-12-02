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


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Class for task-specific configuration properties.
 */
public class BigQuerySinkTaskConfig extends BigQuerySinkConfig {
  private static final ConfigDef config;
  private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkTaskConfig.class);

  public static final String SCHEMA_UPDATE_CONFIG =                     "autoUpdateSchemas";
  private static final ConfigDef.Type SCHEMA_UPDATE_TYPE =              ConfigDef.Type.BOOLEAN;
  public static final Boolean SCHEMA_UPDATE_DEFAULT =                   false;
  private static final ConfigDef.Importance SCHEMA_UPDATE_IMPORTANCE =  ConfigDef.Importance.HIGH;
  private static final String SCHEMA_UPDATE_DOC =
      "Whether or not to automatically update BigQuery schemas";

  public static final String THREAD_POOL_SIZE_CONFIG =                  "threadPoolSize";
  private static final ConfigDef.Type THREAD_POOL_SIZE_TYPE =           ConfigDef.Type.INT;
  public static final Integer THREAD_POOL_SIZE_DEFAULT =                10;
  private static final ConfigDef.Validator THREAD_POOL_SIZE_VALIDATOR = ConfigDef.Range.atLeast(1);
  private static final ConfigDef.Importance THREAD_POOL_SIZE_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String THREAD_POOL_SIZE_DOC =
      "The size of the BigQuery write thread pool. This establishes the maximum number of "
      + "concurrent writes to BigQuery.";

  public static final String TOPIC_MAX_THREADS_CONFIG =               "tableMaxThreads";
  private static final ConfigDef.Type TOPIC_MAX_THREADS_TYPE =        ConfigDef.Type.INT;
  public static final Integer TOPIC_MAX_THREADS_DEFAULT =             7;
  private static final ConfigDef.Validator TOPIC_MAX_THREADS_VALIDATOR =
      ConfigDef.Range.atLeast(1); // also no more than THREAD_POOL_SIZE?
  private static final ConfigDef.Importance TOPIC_MAX_THREADS_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String TOPIC_MAX_THREADS_DOC =
      "The maximum threads that can be writing rows for a single topic before that topic is "
      + "temporarily paused. If there is only a single topic this should be the same as the "
      + "thread pool size.";

  public static final String BIGQUERY_RETRY_CONFIG =                    "bigQueryRetry";
  private static final ConfigDef.Type BIGQUERY_RETRY_TYPE =             ConfigDef.Type.INT;
  public static final Integer BIGQUERY_RETRY_DEFAULT =                  0;
  private static final ConfigDef.Validator BIGQUERY_RETRY_VALIDATOR =   ConfigDef.Range.atLeast(0);
  private static final ConfigDef.Importance BIGQUERY_RETRY_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String BIGQUERY_RETRY_DOC =
      "The number of retry attempts that will be made per BigQuery request that fails with a "
      + "backend error or a quota exceeded error";

  public static final String BIGQUERY_RETRY_WAIT_CONFIG =               "bigQueryRetryWait";
  private static final ConfigDef.Type BIGQUERY_RETRY_WAIT_CONFIG_TYPE = ConfigDef.Type.LONG;
  public static final Long BIGQUERY_RETRY_WAIT_DEFAULT =                1000L;
  private static final ConfigDef.Validator BIGQUERY_RETRY_WAIT_VALIDATOR =
      ConfigDef.Range.atLeast(0);
  private static final ConfigDef.Importance BIGQUERY_RETRY_WAIT_IMPORTANCE =
      ConfigDef.Importance.MEDIUM;
  private static final String BIGQUERY_RETRY_WAIT_DOC =
      "The minimum amount of time, in milliseconds, to wait between BigQuery backend or quota "
      +  "exceeded error retry attempts.";

  static {
    config = BigQuerySinkConfig.getConfig()
        .define(
            SCHEMA_UPDATE_CONFIG,
            SCHEMA_UPDATE_TYPE,
            SCHEMA_UPDATE_DEFAULT,
            SCHEMA_UPDATE_IMPORTANCE,
            SCHEMA_UPDATE_DOC
        ).define(
            THREAD_POOL_SIZE_CONFIG,
            THREAD_POOL_SIZE_TYPE,
            THREAD_POOL_SIZE_DEFAULT,
            THREAD_POOL_SIZE_VALIDATOR,
            THREAD_POOL_SIZE_IMPORTANCE,
            THREAD_POOL_SIZE_DOC
        ).define(
            TOPIC_MAX_THREADS_CONFIG,
            TOPIC_MAX_THREADS_TYPE,
            TOPIC_MAX_THREADS_DEFAULT,
            TOPIC_MAX_THREADS_VALIDATOR,
            TOPIC_MAX_THREADS_IMPORTANCE,
            TOPIC_MAX_THREADS_DOC
        ).define(
            BIGQUERY_RETRY_CONFIG,
            BIGQUERY_RETRY_TYPE,
            BIGQUERY_RETRY_DEFAULT,
            BIGQUERY_RETRY_VALIDATOR,
            BIGQUERY_RETRY_IMPORTANCE,
            BIGQUERY_RETRY_DOC
        ).define(
            BIGQUERY_RETRY_WAIT_CONFIG,
            BIGQUERY_RETRY_WAIT_CONFIG_TYPE,
            BIGQUERY_RETRY_WAIT_DEFAULT,
            BIGQUERY_RETRY_WAIT_VALIDATOR,
            BIGQUERY_RETRY_WAIT_IMPORTANCE,
            BIGQUERY_RETRY_WAIT_DOC
        );
  }

  private void checkAutoUpdateSchemas() {
    Class<?> schemaRetriever = getClass(BigQuerySinkConfig.SCHEMA_RETRIEVER_CONFIG);

    boolean autoUpdateSchemas = getBoolean(SCHEMA_UPDATE_CONFIG);
    if (autoUpdateSchemas && schemaRetriever == null) {
      throw new ConfigException(
          "Cannot specify automatic table creation without a schema retriever"
      );
    }

    if (schemaRetriever == null) {
      logger.warn(
          "No schema retriever class provided; auto schema updates are impossible"
      );
    }
  }

  public static ConfigDef getConfig() {
    return config;
  }

  /**
   * @param properties A Map detailing configuration properties and their respective values.
   */
  public BigQuerySinkTaskConfig(Map<String, String> properties) {
    super(config, properties);
    checkAutoUpdateSchemas();
  }
}
