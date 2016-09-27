package com.wepay.kafka.connect.bigquery.partition;

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
import com.wepay.kafka.connect.bigquery.write.BigQueryWriter;

import org.apache.kafka.connect.data.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * A partitioner that attempts to find the largest non-erroring batch size and then evenly divides
 * all the given elements among the fewest possible batch requests, given the maximum allowable
 * batch size.
 *
 * <p>Similar to {@link EqualPartitioner}, but will dynamically attempt to find the maximum batch
 * size and will resize as needed.
 */
public class DynamicPartitioner implements Partitioner<InsertAllRequest.RowToInsert> {
  private static final Logger logger = LoggerFactory.getLogger(DynamicPartitioner.class);

  // google only allows request batch sizes of up to 100000, so this is a hard maximum.
  private static final int MAXIMUM_BATCH_SIZE = 100000;
  // google suggests a batch size of 500 so it's as good a place to start as any other.
  private static final int INITIAL_BATCH_SIZE = 500;

  private static final int BAD_REQUEST_CODE = 400;

  // in non-seeking mode, the number of writeAlls that must complete successfully without error in
  // a row before we increase the batchSize.
  private static final int CONT_SUCCESS_COUNT_BUMP = 10;

  private BigQueryWriter writer;
  private int currentBatchSize;
  private boolean seeking;

  private int contSuccessCount;

  /**
   * @param writer the {@link BigQueryWriter} that will do the writing.
   */
  public DynamicPartitioner(BigQueryWriter writer) {
    this.writer = writer;
    this.currentBatchSize = INITIAL_BATCH_SIZE;
    this.seeking = true;
    this.contSuccessCount = 0;
  }

  // package private; for testing only
  DynamicPartitioner(BigQueryWriter writer, int initialBatchSize, boolean seeking) {
    this.writer = writer;
    this.currentBatchSize = initialBatchSize;
    this.seeking = seeking;
    this.contSuccessCount = 0;
  }

  // package private; for testing only
  int getCurrentBatchSize() {
    return currentBatchSize;
  }

  @Override
  public void writeAll(TableId table,
                       List<InsertAllRequest.RowToInsert> elements,
                       String topic,
                       Set<Schema> schemas) throws BigQueryConnectException,
                                                   InterruptedException {
    if (seeking) {
      seekingWriteAll(table, elements, topic, schemas);
    } else {
      establishedWriteAll(table, elements, topic, schemas);
    }
  }

  /**
   * A writeAll intended to quickly find the largest viable batchSize.
   *
   * <p>We start with the currentBatchSize and adjust up or down as necessary. Seek mode ends when
   * one of the following occurs:
   *   1. we get an error after at least 1 success. This is slightly inefficient if our
   *      currentBatchSize is too big, but we will only make 1 extra request in that case.
   *      Successes do not carry over from previous seekingWriteAll requests.
   *   2. we write all the given elements in one batch request without error.
   *   3. we write MAXIMUM_BATCH_SIZE elements in one batch request without error. (unlikely)
   */
  private void seekingWriteAll(TableId table,
                               List<InsertAllRequest.RowToInsert> elements,
                               String topic,
                               Set<Schema> schemas) throws BigQueryConnectException,
                                                           InterruptedException {
    logger.debug("Seeking best batch size...");
    int currentIndex = 0;
    int successfulCallCount = 0;
    while (currentIndex < elements.size()) {
      int endIndex = currentIndex + currentBatchSize;
      List<InsertAllRequest.RowToInsert> currentPartition =
          elements.subList(currentIndex, endIndex);
      try {
        writer.writeRows(table, currentPartition, topic, schemas);
        // success
        successfulCallCount++;
        currentIndex = endIndex;
        // if our batch size is the maximum batch size we are done finding a batch size.
        if (currentBatchSize == MAXIMUM_BATCH_SIZE) {
          seeking = false; // case 3
          logger.debug("Best batch size found (max): {}", currentBatchSize);
          establishedWriteAll(table,
                              elements.subList(currentIndex, elements.size()),
                              topic,
                              schemas);
          return;
        }
        //  increase the batch size if: we actually did just test that batchSize
        if (!(currentPartition.size() < currentBatchSize)) {
          increaseBatchSize();
        }
      } catch (BigQueryException exception) {
        // failure
        if (isPartitioningError(exception)) {
          decreaseBatchSize();
          // if we've had at least 1 successful call we'll assume this is a good batch size.
          if (successfulCallCount > 0) {
            seeking = false; // case 1
            logger.debug("Best batch size found (error if higher): {}", currentBatchSize);
            establishedWriteAll(table,
                                elements.subList(currentIndex, elements.size()),
                                topic,
                                schemas);
            return;
          }
        } else {
          throw new BigQueryConnectException(
              String.format("Failed to write to BigQuery table %s", table),
              exception);
        }
      }
    }
    // if we finished all this with only one successful call...
    if (successfulCallCount ==  1) {
      // then we are done finding a batch size.
      seeking = false; // case 2
      // decrease batch size back to the size that we actually checked.
      decreaseBatchSize();
      logger.debug("Best batch size found (all elements): {}", currentBatchSize);
    }
  }

  /**
   * writeAll intended for a generally stable currentBatchSize.
   *
   * <p>Write all the given elements to BigQuery in the smallest possible number of batches, as
   * evenly as possible.
   *
   * <p>If we encounter a partitioning-related error, we will immediately lower the batch size and
   * try again.
   *
   * <p>Every {@link #CONT_SUCCESS_COUNT_BUMP} calls, if there have been no errors, we will bump up
   * the batch size.
   */
  private void establishedWriteAll(TableId table,
                                   List<InsertAllRequest.RowToInsert> elements,
                                   String topic,
                                   Set<Schema> schemas) throws BigQueryConnectException,
                                                               InterruptedException {
    int currentIndex = 0;
    while (currentIndex < elements.size()) {
      try {
        // very similar to equalPartitioner
        // handle case where no partitioning is necessary:
        if (elements.size() <= currentBatchSize) {
          writer.writeRows(table, elements, topic, schemas);
          currentIndex = elements.size();
          return;
        }

        int numPartitions = (int)Math.ceil(elements.size() / (currentBatchSize * 1.0));
        int minBatchSize = elements.size() / numPartitions;
        int spareRows = elements.size() % numPartitions;

        for (int partition = 0; partition < numPartitions; partition++) {
          // the first spareRows partitions have an extra row in them.
          int batchSize = partition < spareRows ? minBatchSize + 1 : minBatchSize;
          int endIndex = currentIndex + batchSize;
          writer.writeRows(table, elements.subList(currentIndex, endIndex), topic, schemas);
          currentIndex = endIndex;
        }
      } catch (BigQueryException exception) {
        if (isPartitioningError(exception)) {
          // immediately decrease batch size and try again with remaining elements.
          decreaseBatchSize();
          contSuccessCount = 0;
          logger.debug("Partition error during establishedWriteAll, reducing batch size to {}",
                       currentBatchSize);
          continue;
        } else {
          throw new BigQueryConnectException(
              String.format("Failed to write to BigQuery table %s", table),
              exception);
        }
      }
    }

    contSuccessCount++;
    if (contSuccessCount >= CONT_SUCCESS_COUNT_BUMP) {
      logger.debug("{} successful establishedWriteAlls in a row, increasing batchSize to {}",
                   contSuccessCount, currentBatchSize);
      increaseBatchSize();
    }
  }

  private boolean isPartitioningError(BigQueryException exception) {
    if (exception.code() == BAD_REQUEST_CODE
        && exception.error() == null
        && exception.reason() == null) {
      // more than 10MB/req
      // todo google may add an error/reason at some point.
      // They probably won't, but it would be nice.
      return true;
    }
    return false;
  }

  // for some reason when I run tests it's asking for these to be public?? I'm not sure why...
  private void increaseBatchSize() {
    currentBatchSize = Math.min(currentBatchSize * 2, MAXIMUM_BATCH_SIZE);
  }

  private void decreaseBatchSize() {
    currentBatchSize = currentBatchSize / 2;
  }
}
