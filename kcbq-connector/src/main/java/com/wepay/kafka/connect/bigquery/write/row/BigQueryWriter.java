package com.wepay.kafka.connect.bigquery.write.row;

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


import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;

import com.wepay.kafka.connect.bigquery.utils.MetricsConstants;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;

import org.apache.kafka.connect.data.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * A class for writing lists of rows to a BigQuery table.
 */
public abstract class BigQueryWriter {

  private static final int FORBIDDEN = 403;
  private static final int INTERNAL_SERVICE_ERROR = 500;
  private static final int BAD_GATEWAY = 502;
  private static final int SERVICE_UNAVAILABLE = 503;
  private static final String QUOTA_EXCEEDED_REASON = "quotaExceeded";
  private static final String RATE_LIMIT_EXCEEDED_REASON = "rateLimitExceeded";

  private static final int WAIT_MAX_JITTER = 1000;

  private static final Logger logger = LoggerFactory.getLogger(BigQueryWriter.class);

  private static final Random random = new Random();

  private final Sensor rowsWritten;
  private final Sensor requestRetries;

  private int retries;
  private long retryWaitMs;

  /**
   * @param retries the number of times to retry a request if BQ returns an internal service error
   *                or a service unavailable error.
   * @param retryWaitMs the amount of time to wait in between reattempting a request if BQ returns
   *                    an internal service error or a service unavailable error.
   * @param metrics kafka {@link Metrics}.
   */
  public BigQueryWriter(int retries, long retryWaitMs, Metrics metrics) {
    this.retries = retries;
    this.retryWaitMs = retryWaitMs;

    rowsWritten = metrics.sensor("rows-written");
    rowsWritten.add(metrics.metricName("rows-written-avg",
                                       MetricsConstants.groupName,
                                       "The average number of rows written per request"),
                    new Avg());
    rowsWritten.add(metrics.metricName("rows-written-max",
                                       MetricsConstants.groupName,
                                       "The maximum number of rows written per request"),
                    new Max());
    rowsWritten.add(metrics.metricName("rows-written-rate",
                                       MetricsConstants.groupName,
                                       "The average number of rows written per second"),
                    new Rate());

    requestRetries = metrics.sensor("request-retries");
    requestRetries.add(metrics.metricName("request-retries-avg",
                                          MetricsConstants.groupName,
                                          "The average number of retries per request"),
                       new Avg());
    requestRetries.add(metrics.metricName("request-retries-max",
                                          MetricsConstants.groupName,
                                          "The maximum number of retry attempts made for a single "
                                          + "request"),
                       new Max());
    requestRetries.add(metrics.metricName("request-retries-count",
                                          MetricsConstants.groupName,
                                          "The total number of retry attempts made"),
                       new Count());
  }

  /**
   * Handle the actual transmission of the write request to BigQuery, including any exceptions or
   * errors that happen as a result.
   * @param tableId The PartitionedTableId.
   * @param rows The rows to write.
   * @param topic The Kafka topic that the row data came from.
   * @param schemas The unique Schemas for the row data.
   */
  protected abstract void performWriteRequest(PartitionedTableId tableId,
                                              List<InsertAllRequest.RowToInsert> rows,
                                              String topic,
                                              Set<Schema> schemas)
      throws BigQueryException, BigQueryConnectException;

  /**
   * Create an InsertAllRequest.
   * @param tableId the table to insert into.
   * @param rows the rows to insert.
   * @return the InsertAllRequest.
   */
  protected InsertAllRequest createInsertAllRequest(PartitionedTableId tableId,
                                                    List<InsertAllRequest.RowToInsert> rows) {
    return InsertAllRequest.builder(tableId.getFullTableId(), rows)
        .ignoreUnknownValues(false)
        .skipInvalidRows(false)
        .build();
  }

  /**
   * @param table The BigQuery table to write the rows to.
   * @param rows The rows to write.
   * @param topic The Kafka topic that the row data came from.
   * @param schemas The unique Schemas for the row data.
   * @throws InterruptedException if interrupted.
   */
  public void writeRows(PartitionedTableId table,
                        List<InsertAllRequest.RowToInsert> rows,
                        String topic,
                        Set<Schema> schemas)
      throws BigQueryConnectException, BigQueryException, InterruptedException {
    logger.debug("writing {} row{} to table {}", rows.size(), rows.size() != 1 ? "s" : "", table);

    Exception mostRecentException = null;

    int retryCount = 0;
    do {
      if (retryCount > 0) {
        waitRandomTime();
      }
      try {
        performWriteRequest(table, rows, topic, schemas);
        requestRetries.record(retryCount);
        rowsWritten.record(rows.size());
        return;
      } catch (BigQueryException err) {
        mostRecentException = err;
        if (err.code() == INTERNAL_SERVICE_ERROR
            || err.code() == SERVICE_UNAVAILABLE
            || err.code() == BAD_GATEWAY) {
          // backend error: https://cloud.google.com/bigquery/troubleshooting-errors
          /* for BAD_GATEWAY: https://cloud.google.com/storage/docs/json_api/v1/status-codes
             todo possibly this page is inaccurate for bigquery, but the message we are getting
             suggest it's an internal backend error and we should retry, so lets take that at face
             value. */
          logger.warn("BQ backend error: {}", err.code());
          retryCount++;
        } else if (err.code() == FORBIDDEN
                   && err.error() != null
                   && QUOTA_EXCEEDED_REASON.equals(err.reason())) {
          // quota exceeded error
          logger.warn("Quota exceeded for table {}", table);
          retryCount++;
        } else if (err.code() == FORBIDDEN
                   && err.error() != null
                   && RATE_LIMIT_EXCEEDED_REASON.equals(err.reason())) {
          // rate limit exceeded error
          logger.warn("Rate limit exceeded for table {}", table);
          retryCount++;
        } else {
          throw err;
        }
      }
    } while (retryCount <= retries);
    requestRetries.record(retryCount);
    throw new BigQueryConnectException(
        String.format("Exceeded configured %d attempts for write request", retries),
        mostRecentException);
  }

  /**
   * Wait at least {@link #retryWaitMs}, with up to an additional 1 second of random jitter.
   * @throws InterruptedException if interrupted.
   */
  private void waitRandomTime() throws InterruptedException {
    // wait
    Thread.sleep(retryWaitMs + random.nextInt(WAIT_MAX_JITTER));
  }
}
