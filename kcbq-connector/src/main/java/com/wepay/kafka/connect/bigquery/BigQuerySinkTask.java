package com.wepay.kafka.connect.bigquery;

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


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;

import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.exception.SinkConfigConnectException;

import com.wepay.kafka.connect.bigquery.utils.MetricsConstants;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.utils.TopicToTableResolver;
import com.wepay.kafka.connect.bigquery.utils.Version;

import com.wepay.kafka.connect.bigquery.write.batch.BatchWriter;

import com.wepay.kafka.connect.bigquery.write.batch.KCBQThreadPoolExecutor;
import com.wepay.kafka.connect.bigquery.write.batch.TableWriter;
import com.wepay.kafka.connect.bigquery.write.row.AdaptiveBigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;
import com.wepay.kafka.connect.bigquery.write.row.SimpleBigQueryWriter;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigException;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

/**
 * A {@link SinkTask} used to translate Kafka Connect {@link SinkRecord SinkRecords} into BigQuery
 * {@link RowToInsert RowToInserts} and subsequently write them to BigQuery.
 */
public class BigQuerySinkTask extends SinkTask {
  private static final Logger logger = LoggerFactory.getLogger(BigQuerySinkTask.class);

  private final BigQuery testBigQuery;
  private BigQueryWriter bigQueryWriter;
  private BigQuerySinkTaskConfig config;
  private RecordConverter<Map<String, Object>> recordConverter;
  private Map<String, TableId> topicsToBaseTableIds;

  private KCBQThreadPoolExecutor executor;

  private Metrics metrics;
  private Sensor rowsRead;

  public BigQuerySinkTask() {
    testBigQuery = null;
  }

  // For testing purposes only; will never be called by the Kafka Connect framework
  BigQuerySinkTask(BigQuery testBigQuery) {
    this.testBigQuery = testBigQuery;
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    try {
      executor.awaitCurrentTasks();
    } catch (InterruptedException e) {
      throw new ConnectException("Interrupted while waiting for write tasks to complete.");
    }
    updateOffsets(offsets);
  }

  /**
   * This really doesn't do much and I'm not totally clear on whether or not I need it.
   * But, in the interest of maintaining old functionality, here we are.
   */
  private void updateOffsets(Map<TopicPartition, OffsetAndMetadata> offsets) {
    for (Map.Entry<TopicPartition, OffsetAndMetadata> offsetEntry : offsets.entrySet()) {
      context.offset(offsetEntry.getKey(), offsetEntry.getValue().offset());
    }
  }

  private PartitionedTableId getRecordTable(SinkRecord record) {
    TableId baseTableId = topicsToBaseTableIds.get(record.topic());
    return new PartitionedTableId.Builder(baseTableId).setDayPartitionForNow().build();
  }

  private String getRowId(SinkRecord record) {
    return String.format("%s-%d-%d",
                         record.topic(),
                         record.kafkaPartition(),
                         record.kafkaOffset());
  }

  private RowToInsert getRecordRow(SinkRecord record) {
    return RowToInsert.of(
      getRowId(record),
      recordConverter.convertRecord(record)
    );
  }

  @Override
  public void put(Collection<SinkRecord> records) {
    Map<PartitionedTableId, TableWriter.Builder> tableWriterBuilders = new HashMap<>();

    for (SinkRecord record : records) {
      if (record.value() != null) {
        PartitionedTableId table = getRecordTable(record);
        if (!tableWriterBuilders.containsKey(table)) {
          TableWriter.Builder tableWriterBuilder =
            new TableWriter.Builder(bigQueryWriter, table, record.topic());
          tableWriterBuilders.put(table, tableWriterBuilder);
        }
        tableWriterBuilders.get(table).addRow(getRecordRow(record));
      }
    }

    for (TableWriter.Builder builder : tableWriterBuilders.values()) {
      executor.execute(builder.build());
    }
  }

  private RecordConverter<Map<String, Object>> getConverter() {
    return config.getRecordConverter();
  }

  private BigQuery getBigQuery() {
    if (testBigQuery != null) {
      return testBigQuery;
    }
    String projectName = config.getString(config.PROJECT_CONFIG);
    String keyFilename = config.getString(config.KEYFILE_CONFIG);
    return new BigQueryHelper().connect(projectName, keyFilename);
  }

  private SchemaManager getSchemaManager(BigQuery bigQuery) {
    SchemaRetriever schemaRetriever = config.getSchemaRetriever();
    SchemaConverter<com.google.cloud.bigquery.Schema> schemaConverter =
        config.getSchemaConverter();
    return new SchemaManager(schemaRetriever, schemaConverter, bigQuery);
  }

  private BigQueryWriter getBigQueryWriter() {
    boolean updateSchemas = config.getBoolean(config.SCHEMA_UPDATE_CONFIG);
    int retry = config.getInt(config.BIGQUERY_RETRY_CONFIG);
    long retryWait = config.getLong(config.BIGQUERY_RETRY_WAIT_CONFIG);
    BigQuery bigQuery = getBigQuery();
    if (updateSchemas) {
      return new AdaptiveBigQueryWriter(bigQuery,
                                        getSchemaManager(bigQuery),
                                        retry,
                                        retryWait,
                                        metrics);
    } else {
      return new SimpleBigQueryWriter(bigQuery, retry, retryWait, metrics);
    }
  }

  private void configureMetrics() {
    metrics = new Metrics();
    rowsRead = metrics.sensor("rows-read");
    rowsRead.add(metrics.metricName("rows-read-avg",
                                    MetricsConstants.groupName,
                                    "The average number of rows written per request"),
                 new Avg());
    rowsRead.add(metrics.metricName("rows-read-max",
                                    MetricsConstants.groupName,
                                    "The maximum number of rows written per request"),
                 new Max());
    rowsRead.add(metrics.metricName("rows-read-rate",
                                    MetricsConstants.groupName,
                                    "The average number of rows written per second"),
                 new Rate());
  }

  @Override
  public void start(Map<String, String> properties) {
    logger.trace("task.start()");
    try {
      config = new BigQuerySinkTaskConfig(properties);
    } catch (ConfigException err) {
      throw new SinkConfigConnectException(
          "Couldn't start BigQuerySinkTask due to configuration error",
          err
      );
    }

    configureMetrics();

    bigQueryWriter = getBigQueryWriter();
    topicsToBaseTableIds = TopicToTableResolver.getTopicsToTables(config);
    recordConverter = getConverter();
    executor = new KCBQThreadPoolExecutor(config, context, new LinkedBlockingQueue<>());
  }

  @Override
  public void stop() {
    logger.trace("task.stop()");
  }

  @Override
  public String version() {
    String version = Version.version();
    logger.trace("task.version() = {}", version);
    return version;
  }
}
