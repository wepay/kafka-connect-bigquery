package com.wepay.kafka.connect.bigquery.write.batch;

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
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;

import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import com.wepay.kafka.connect.bigquery.exception.ExpectedInterruptException;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Consumer;

/**
 * Simple Table Writer that attempts to write all the rows it is given at once.
 */
public class TableWriter implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(TableWriter.class);

  private static final int BAD_REQUEST_CODE = 400;
  private static final String INVALID_REASON = "invalid";

  private final BigQueryWriter writer;
  private final PartitionedTableId table;
  private final List<RowToInsert> rows;
  private final String topic;
  private final Consumer<List<RowToInsert>> onFinish;

  /**
   * @param writer the {@link BigQueryWriter} to use.
   * @param table the BigQuery table to write to.
   * @param rows the rows to write.
   * @param topic the kafka source topic of this data.
   * @param onFinish a callback to invoke after all rows have been written successfully, which is
   *                 called with all the rows written by the writer
   */
  public TableWriter(BigQueryWriter writer,
                     PartitionedTableId table,
                     List<RowToInsert> rows,
                     String topic,
                     Consumer<List<RowToInsert>> onFinish) {
    this.writer = writer;
    this.table = table;
    this.rows = rows;
    this.topic = topic;
    this.onFinish = onFinish;
  }

  @Override
  public void run() {
    int currentIndex = 0;
    int currentBatchSize = rows.size();
    int successCount = 0;
    int failureCount = 0;

    try {
      while (currentIndex < rows.size()) {
        List<RowToInsert> currentBatch =
            rows.subList(currentIndex, Math.min(currentIndex + currentBatchSize, rows.size()));
        try {
          writer.writeRows(table, currentBatch, topic);
          currentIndex += currentBatchSize;
          successCount++;
        } catch (BigQueryException err) {
          logger.warn("Could not write batch of size {} to BigQuery.", currentBatch.size(), err);
          if (isBatchSizeError(err)) {
            failureCount++;
            currentBatchSize = getNewBatchSize(currentBatchSize);
          }
        }
      }
    } catch (InterruptedException err) {
      throw new ExpectedInterruptException("Thread interrupted while writing to BigQuery.");
    }

    // Common case is 1 successful call and 0 failed calls:
    // Write to info if uncommon case,
    // Write to debug if common case
    String logMessage = "Wrote {} rows over {} successful calls and {} failed calls.";
    if (successCount + failureCount > 1) {
      logger.info(logMessage, rows.size(), successCount, failureCount);
    } else {
      logger.debug(logMessage, rows.size(), successCount, failureCount);
    }

    onFinish.accept(rows);
  }

  private static int getNewBatchSize(int currentBatchSize) {
    if (currentBatchSize == 1) {
      // todo correct exception type?
      throw new ConnectException("Attempted to reduce batch size below 1.");
    }
    // round batch size up so we don't end up with a dangling 1 row at the end.
    return (int) Math.ceil(currentBatchSize / 2.0);
  }

  /**
   * @param exception the {@link BigQueryException} to check.
   * @return true if this error is an error that can be fixed by retrying with a smaller batch
   *         size, or false otherwise.
   */
  private static boolean isBatchSizeError(BigQueryException exception) {
    if (exception.getCode() == BAD_REQUEST_CODE
        && exception.getError() == null
        && exception.getReason() == null) {
      /*
       * 400 with no error or reason represents a request that is more than 10MB. This is not
       * documented but is referenced slightly under "Error codes" here:
       * https://cloud.google.com/bigquery/quota-policy
       * (by decreasing the batch size we can eventually expect to end up with a request under 10MB)
       */
      return true;
    } else if (exception.getCode() == BAD_REQUEST_CODE
               && INVALID_REASON.equals(exception.getReason())) {
      /*
       * this is the error that the documentation claims google will return if a request exceeds
       * 10MB. if this actually ever happens...
       * todo distinguish this from other invalids (like invalid table schema).
       */
      return true;
    }
    return false;
  }

  public String getTopic() {
    return topic;
  }

  public static class Builder implements TableWriterBuilder {
    private final BigQueryWriter writer;
    private final PartitionedTableId table;
    private final String topic;

    private Consumer<List<RowToInsert>> onFinish;
    private List<RowToInsert> rows;

    /**
     * @param writer the BigQueryWriter to use
     * @param table the BigQuery table to write to.
     * @param topic the kafka source topic associated with the given table.
     */
    public Builder(BigQueryWriter writer, PartitionedTableId table, String topic) {
      this.writer = writer;
      this.table = table;
      this.topic = topic;

      this.onFinish = null;
      this.rows = new ArrayList<>();
    }

    /**
     * Add a record to the builder.
     * @param rowToInsert the row to add
     */
    public void addRow(RowToInsert rowToInsert) {
      rows.add(rowToInsert);
    }

    /**
     * Specify a callback to be invoked after all rows have been written. The callback will be
     * invoked with the full list of rows written by this table writer.
     * @param onFinish the callback to invoke; may not be null
     * @throws IllegalStateException if invoked more than once on a single builder instance
     */
    public void onFinish(Consumer<List<RowToInsert>> onFinish) {
      if (this.onFinish != null) {
        throw new IllegalStateException("Cannot overwrite existing finish callback");
      }
      this.onFinish = Objects.requireNonNull(onFinish, "Finish callback cannot be null");
    }

    /**
     * Create a {@link TableWriter} from this builder.
     * @return a TableWriter containing the given writer, table, topic, and all added rows.
     */
    public TableWriter build() {
      return new TableWriter(writer, table, rows, topic, onFinish != null ? onFinish : n -> { });
    }
  }
}
