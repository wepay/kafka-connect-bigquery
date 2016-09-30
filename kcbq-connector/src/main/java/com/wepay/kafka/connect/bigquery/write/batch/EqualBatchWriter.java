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
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;
import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.Set;

/**
 * A partitioner that divides elements into a series of partitions such that two conditions are
 * achieved: firstly, the largest writeAll and the smallest writeAll differ in length by no more
 * one row, and secondly, no writeAll shall be larger in size than an amount specified during
 * instantiation.
 */
public class EqualBatchWriter implements BatchWriter<InsertAllRequest.RowToInsert> {
  private final int maxPartitionSize;
  private BigQueryWriter writer;

  /**
   * @param writer the {@link BigQueryWriter}
   * @param maxPartitionSize The maximum size of a writeAll returned by a call to writeAll().
   */
  public EqualBatchWriter(BigQueryWriter writer, int maxPartitionSize) {
    if (maxPartitionSize <= 0) {
      throw new IllegalArgumentException("Maximum size of writeAll must be a positive number");
    }
    this.writer = writer;
    this.maxPartitionSize = maxPartitionSize;
  }

  /**
   * Take a list of rows to insert, and divide it up among potentially several BigQuery write
   * requests, where each write request contains no more than maxPartitionSize rows.
   *
   * @param elements The list of elements to partition.
   */
  @Override
  public void writeAll(TableId table,
                       List<InsertAllRequest.RowToInsert> elements,
                       String topic,
                       Set<Schema> schemas) throws BigQueryConnectException, InterruptedException {
    // Handle the case where no partitioning is necessary
    if (elements.size() <= maxPartitionSize) {
      try {
        writer.writeRows(table, elements, topic, schemas);
      } catch (BigQueryException err) {
        throw new BigQueryConnectException(
          String.format("Failed to write to BigQuery table %s", table),
          err);
      }
      return;
    }

    // Ceiling division
    int numPartitions = (elements.size() + maxPartitionSize - 1) / maxPartitionSize;
    int minPartitionSize = elements.size() / numPartitions;

    // The beginning of the next writeAll, as an index within <rows>
    int partitionStart = 0;
    for (int partition = 0; partition < numPartitions; partition++) {
      // If every writeAll were given <minPartitionSize> rows, there would be exactly
      // <numRows> % <numPartitions> rows left over.
      // As a result, the first (<numRows> % <numPartitions>) partitions are given
      // (<minPartitionSize> + 1) rows, and the rest are given <minPartitionSize> rows.
      int partitionSize =
          partition < elements.size() % numPartitions ? minPartitionSize + 1 : minPartitionSize;

      // The end of the next writeAll, within <rows>
      // IndexOutOfBoundsExceptions shouldn't occur here, but just to make sure, guarantee that the
      // end of the writeAll isn't past the end of the array of rows.
      int partitionEnd = Math.min(partitionStart + partitionSize, elements.size());

      // Add the next writeAll to the return value
      try {
        writer.writeRows(table, elements.subList(partitionStart, partitionEnd), topic, schemas);
      } catch (BigQueryException err) {
        throw new BigQueryConnectException(
            String.format("Failed to write to BigQuery table %s", table),
            err);
      }
      partitionStart += partitionSize;
    }
  }
}
