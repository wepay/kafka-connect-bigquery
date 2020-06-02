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


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;

import java.util.Map;
import java.util.concurrent.Future;

public class UpsertDeleteBigQueryWriter extends AdaptiveBigQueryWriter {

  private final SchemaManager schemaManager;
  private final boolean autoCreateTables;
  private final Map<TableId, TableId> intermediateToDestinationTables;

  /**
   * @param bigQuery Used to send write requests to BigQuery.
   * @param schemaManager Used to update BigQuery tables.
   * @param retry How many retries to make in the event of a 500/503 error.
   * @param retryWait How long to wait in between retries.
   * @param autoUpdateSchemas Whether destination table schemas should be automatically updated
   * @param autoCreateTables Whether destination tables should be automatically created
   * @param intermediateToDestinationTables A mapping used to determine the destination table for
   *                                        given intermediate tables; used for create/update
   *                                        operations in order to propagate them to the destination
   *                                        table
   */
  public UpsertDeleteBigQueryWriter(BigQuery bigQuery,
                                    SchemaManager schemaManager,
                                    int retry,
                                    long retryWait,
                                    boolean autoUpdateSchemas,
                                    boolean autoCreateTables,
                                    Map<TableId, TableId> intermediateToDestinationTables) {
    // Hardcode autoCreateTables to true in the superclass so that intermediate tables will be
    // automatically created
    // The super class will handle all of the logic for writing to, creating, and updating
    // intermediate tables; this class will handle logic for creating/updating the destination table
    super(bigQuery, schemaManager.forIntermediateTables(), retry, retryWait, autoUpdateSchemas, true);
    this.schemaManager = schemaManager;
    this.autoCreateTables = autoCreateTables;
    this.intermediateToDestinationTables = intermediateToDestinationTables;
  }

  @Override
  protected void attemptSchemaUpdate(PartitionedTableId tableId, String topic) {
    // Update the intermediate table here...
    super.attemptSchemaUpdate(tableId, topic);
    try {
      // ... and update the destination table here
      schemaManager.updateSchema(intermediateToDestinationTables.get(tableId.getBaseTableId()), topic);
    } catch (BigQueryException exception) {
      throw new BigQueryConnectException(
          "Failed to update destination table schema for: " + tableId.getBaseTableId(), exception);
    }
  }

  @Override
  protected void attemptTableCreate(TableId tableId, String topic) {
    // Create the intermediate table here...
    super.attemptTableCreate(tableId, topic);
    if (autoCreateTables) {
      try {
        // ... and create or update the destination table here, if it doesn't already exist and auto
        // table creation is enabled
        schemaManager.createOrUpdateTable(intermediateToDestinationTables.get(tableId), topic);
      } catch (BigQueryException exception) {
        throw new BigQueryConnectException(
            "Failed to create table " + tableId, exception);
      }
    }
  }
}
