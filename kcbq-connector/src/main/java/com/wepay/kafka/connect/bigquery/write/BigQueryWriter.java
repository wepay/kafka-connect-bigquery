package com.wepay.kafka.connect.bigquery.write;

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
import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;

import com.wepay.kafka.connect.bigquery.utils.MetricsConstants;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * A class for writing lists of rows to a BigQuery table.
 */
public abstract class BigQueryWriter {

  private static final int QUOTA_EXCEEDED = 403;
  // note: 403 covers more than just quota exceeded:
  // https://cloud.google.com/bigquery/troubleshooting-errors
  private static final int INTERNAL_SERVICE_ERROR = 500;
  private static final int SERVICE_UNAVAILABLE = 503;

  public static final long QUOTA_EXCEEDED_MIN_WAIT = 1000L;
  public static final int QUOTA_EXCEEDED_MAX_EXTRA = 1000;

  private static final Logger logger = LoggerFactory.getLogger(BigQueryWriter.class);

  private static final Random random = new Random();

  public final Sensor rowsWritten;

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
  }

  /**
   * Handle the actual transmission of the write request to BigQuery, including any exceptions or
   * errors that happen as a result.
   * @param request The request to send to BigQuery.
   * @param topic The Kafka topic that the row data came from.
   * @param schemas The unique Schemas for the row data.
   */
  protected abstract void performWriteRequest(
      InsertAllRequest request,
      String topic,
      Set<Schema> schemas
  );

  /**
   * @param table The BigQuery table to write the rows to.
   * @param rows The rows to write.
   * @param topic The Kafka topic that the row data came from.
   * @param schemas The unique Schemas for the row data.
   * @throws InterruptedException if interrupted.
   */
  public void writeRows(
      TableId table,
      List<InsertAllRequest.RowToInsert> rows,
      String topic,
      Set<Schema> schemas) throws InterruptedException {
    logger.debug("writing {} row{} to table {}", rows.size(), rows.size() != 1 ? "s" : "", table);

    int retryCount = 0;
    do {
      if (retryCount > 0) {
        try {
          Thread.sleep(retryWaitMs);
        } catch (InterruptedException err) {
          throw new ConnectException("Interrupted while waiting for BigQuery retry", err);
        }
      }
      try {
        performWriteRequest(
            InsertAllRequest.builder(table, rows)
                .ignoreUnknownValues(false)
                .skipInvalidRows(false)
                .build(),
            topic,
            schemas
        );
        return;
      } catch (BigQueryException err) {
        if (err.code() == INTERNAL_SERVICE_ERROR || err.code() == SERVICE_UNAVAILABLE) {
          // backend error: https://cloud.google.com/bigquery/troubleshooting-errors
          retryCount++;
        } else if (err.code() == QUOTA_EXCEEDED) {
          // wait, retry; don't count against retryCount
          waitRandomTime();
        } else {
          throw new BigQueryConnectException("Failed to write to BigQuery table " + table, err);
        }
      }
    } while (retryCount <= retries);
  }

  /**
   * Wait a random amount of time between 1000ms and 2000ms.
   * @throws InterruptedException if interrupted.
   */
  public void waitRandomTime() throws InterruptedException {
    Thread.sleep(QUOTA_EXCEEDED_MIN_WAIT + random.nextInt(QUOTA_EXCEEDED_MAX_EXTRA));
  }
}
