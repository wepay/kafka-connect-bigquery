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


import com.google.cloud.bigquery.TableId;

import java.time.LocalDate;

/**
 * A TableId with separate base table name and partition information.
 */
public class PartitionedTableId {

  private static final String PARTITION_DELIMITER = "$";

  private final String project;
  private final String dataset;
  private final String table;
  private final String partition;

  private final String fullTableName;

  private final TableId partialTableId;
  private final TableId fullTableId;

  /**
   * Create a new {@link PartitionedTableId}
   *
   * @param project The project name, if specified.
   * @param dataset The dataset name.
   * @param table The table name.
   * @param partition The partition of the table, if any.
   */
  private PartitionedTableId(String project, String dataset, String table, String partition) {
    this.project = project;
    this.dataset = dataset;
    this.table = table;
    this.partition = partition;

    fullTableName = createFullTableName(table, partition);

    partialTableId = createTableId(project, dataset, table);
    fullTableId = createTableId(project, dataset, fullTableName);
  }

  /**
   * Create a full table name from a base table name and a partition.
   * @param table the base table name.
   * @param partition the partition, if any.
   * @return
   */
  private static String createFullTableName(String table, String partition) {
    if (partition == null) {
      return table;
    } else {
      return table + PARTITION_DELIMITER + partition;
    }
  }

  /**
   * Convenience method for creating new TableIds.
   * <p>
   * {@link TableId#of(String, String, String)} will error if you pass in a null project, so this
   * just checks if the project is null and calls the correct method.
   *
   * @param project the project name, or null
   * @param dataset the dataset name
   * @param table the table name
   * @return a new TableId with the given project, dataset, and table.
   */
  private static TableId createTableId(String project, String dataset, String table) {
    if (project == null) {
      return TableId.of(dataset, table);
    } else {
      return TableId.of(project, dataset, table);
    }
  }

  /**
   * @return the project name, if any.
   */
  public String getProject() {
    return project;
  }

  /**
   * @return the dataset name.
   */
  public String getDataset() {
    return dataset;
  }

  /**
   * @return the base table name.
   */
  public String getBaseTableName() {
    return table;
  }

  /**
   * @return the partition, if any.
   */
  public String getPartition() {
    return partition;
  }

  /**
   * @return the full table name.
   */
  public String getFullTableName() {
    return fullTableName;
  }

  /**
   * @return the partial table id.
   */
  public TableId getPartialTableId() {
    return partialTableId;
  }

  /**
   * @return the full table id.
   */
  public TableId getFullTableId() {
    return fullTableId;
  }

  public static class Builder {

    private String project;
    private final String dataset;
    private final String baseTable;
    private String partition;

    public Builder(String dataset, String baseTable) {
      this.project = null;
      this.dataset = dataset;
      this.baseTable = baseTable;
      this.partition = null;
    }

    public Builder(TableId baseTableId) {
      this.project = baseTableId.project();
      this.dataset = baseTableId.dataset();
      this.baseTable = baseTableId.table();
    }

    public Builder setProject(String project) {
      this.project = project;
      return this;
    }

    public Builder setPartition(String partition) {
      this.partition = partition;
      return this;
    }

    public Builder setDayPartition(LocalDate localDate) {
      return setPartition(dateToDayPartition(localDate));
    }

    /**
     * @param localDate the localDate of the partition.
     * @return The String representation of the partition.
     */
    private static String dateToDayPartition(LocalDate localDate) {
      return "" + localDate.getYear() + localDate.getMonthValue() + localDate.getDayOfMonth();
    }

    /**
     * Build the {@link PartitionedTableId}.
     *
     * @return a {@link PartitionedTableId}.
     */
    public PartitionedTableId build() {
      return new PartitionedTableId(project, dataset, baseTable, partition);
    }
  }
}
