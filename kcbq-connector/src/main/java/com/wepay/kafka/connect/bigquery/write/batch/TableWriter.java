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


import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;

import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;
import com.wepay.kafka.connect.bigquery.write.row.BigQueryWriter;

import org.apache.kafka.connect.errors.ConnectException;

import java.util.ArrayList;
import java.util.List;

/**
 * Simple Table Writer that attempts to write all the rows it is given at once.
 */
public class TableWriter implements Runnable {
  private final BigQueryWriter writer;
  private final PartitionedTableId table;
  private final List<RowToInsert> rows;
  private final String topic;

  /**
   * @param writer the {@link BigQueryWriter} to use.
   * @param table the BigQuery table to write to.
   * @param rows the rows to write.
   * @param topic the kafka source topic of this data.
   */
  public TableWriter(BigQueryWriter writer,
                     PartitionedTableId table,
                     List<RowToInsert> rows,
                     String topic) {
    this.writer = writer;
    this.table = table;
    this.rows = rows;
    this.topic = topic;
  }

  @Override
  public void run() {
    try {
      writer.writeRows(table, rows, topic);
      // todo do something if the request was too big.
    } catch (InterruptedException err) {
      throw new ConnectException("Thread interrupted while writing to BigQuery.", err);
    }
  }

  public String getTopic() {
    return topic;
  }

  public static class Builder {
    private final BigQueryWriter writer;
    private final PartitionedTableId table;
    private final String topic;

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

      this.rows = new ArrayList<>();
    }

    /**
     * Add a row to the builder.
     * @param rowToInsert the rows to add.
     */
    public void addRow(RowToInsert rowToInsert) {
      rows.add(rowToInsert);
    }

    /**
     * Create a {@link TableWriter} from this builder.
     * @return a TableWriter containing the given writer, table, topic, and all added rows.
     */
    public TableWriter build() {
      return new TableWriter(writer, table, rows, topic);
    }
  }
}
