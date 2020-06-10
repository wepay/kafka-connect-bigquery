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
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;

import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;

import com.wepay.kafka.connect.bigquery.utils.PartitionedTableId;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link BigQueryWriter} capable of updating BigQuery table schemas and creating non-existed tables automatically.
 */
public class AdaptiveBigQueryWriter extends BigQueryWriter {
  private static final Logger logger = LoggerFactory.getLogger(AdaptiveBigQueryWriter.class);

  // The maximum number of retries we will attempt to write rows after creating a table or updating a BQ table schema.
  private static final int RETRY_LIMIT = 5;
  // Wait for about 30s between each retry since both creating table and updating schema take up to 2~3 minutes to take effect.
  private static final int RETRY_WAIT_TIME = 30000;

  private final BigQuery bigQuery;
  private final SchemaManager schemaManager;
  private final boolean autoUpdateSchemas;
  private final boolean autoCreateTables;

  /**
   * @param bigQuery Used to send write requests to BigQuery.
   * @param schemaManager Used to update BigQuery tables.
   * @param retry How many retries to make in the event of a 500/503 error.
   * @param retryWait How long to wait in between retries.
   */
  public AdaptiveBigQueryWriter(BigQuery bigQuery,
                                SchemaManager schemaManager,
                                int retry,
                                long retryWait,
                                boolean autoUpdateSchemas,
                                boolean autoCreateTables) {
    super(retry, retryWait);
    this.bigQuery = bigQuery;
    this.schemaManager = schemaManager;
    this.autoUpdateSchemas = autoUpdateSchemas;
    this.autoCreateTables = autoCreateTables;
  }

  private boolean isTableMissingSchema(BigQueryException exception) {
    // If a table is missing a schema, it will raise a BigQueryException that the input is invalid
    // For more information about BigQueryExceptions, see: https://cloud.google.com/bigquery/troubleshooting-errors
    return exception.getReason() != null && exception.getReason().equalsIgnoreCase("invalid");
  }

  private boolean isTableNotExistedException(BigQueryException exception) {
    // If a table does not exist, it will raise a BigQueryException that the input is notFound
    // Referring to Google Cloud Error Codes Doc: https://cloud.google.com/bigquery/docs/error-messages?hl=en
    return exception.getCode() == 404;
  }

  /**
   * Sends the request to BigQuery, then checks the response to see if any errors have occurred. If
   * any have, and all errors can be blamed upon invalid columns in the rows sent, attempts to
   * update the schema of the table in BigQuery and then performs the same write request.
   * @see BigQueryWriter#performWriteRequest(PartitionedTableId, List, String)
   */
  @Override
  public Map<Long, List<BigQueryError>> performWriteRequest(
      PartitionedTableId tableId,
      List<InsertAllRequest.RowToInsert> rows,
      String topic) {
    InsertAllResponse writeResponse = null;
    InsertAllRequest request = null;

    try {
      request = createInsertAllRequest(tableId, rows);
      writeResponse = bigQuery.insertAll(request);
      // Should only perform one schema update attempt.
      if (writeResponse.hasErrors()
              && onlyContainsInvalidSchemaErrors(writeResponse.getInsertErrors())) {
        if (autoUpdateSchemas) {
          attemptSchemaUpdate(tableId, topic);
        } else {
          return writeResponse.getInsertErrors();
        }
      }
    } catch (BigQueryException exception) {
      // Should only perform one table creation attempt.
      if (isTableNotExistedException(exception) && autoCreateTables && bigQuery.getTable(tableId.getBaseTableId()) == null) {
        attemptTableCreate(tableId.getBaseTableId(), topic);
      } else if (isTableMissingSchema(exception) && autoUpdateSchemas) {
        attemptSchemaUpdate(tableId, topic);
      } else {
        throw exception;
      }
    }

    // Creating tables or updating table schemas in BigQuery takes up to 2~3 minutes to take affect,
    // so multiple insertion attempts may be necessary.
    int attemptCount = 0;
    while (writeResponse == null || writeResponse.hasErrors()) {
      logger.trace("insertion failed");
      if (writeResponse == null
          || onlyContainsInvalidSchemaErrors(writeResponse.getInsertErrors())) {
        try {
          // If the table was missing its schema, we never received a writeResponse
          logger.debug("re-attempting insertion");
          writeResponse = bigQuery.insertAll(request);
        } catch (BigQueryException exception) {
          // no-op, we want to keep retrying the insert
        }
      } else {
        return writeResponse.getInsertErrors();
      }
      attemptCount++;
      if (attemptCount >= RETRY_LIMIT) {
        throw new BigQueryConnectException(
            "Failed to write rows after BQ table creation or schema update within "
                + RETRY_LIMIT + " attempts for: " + tableId.getBaseTableId());
      }
      try {
        Thread.sleep(RETRY_WAIT_TIME);
      } catch (InterruptedException e) {
        // no-op, we want to keep retrying the insert
      }
    }
    logger.debug("table insertion completed successfully");
    return new HashMap<>();
  }

  private void attemptSchemaUpdate(PartitionedTableId tableId, String topic) {
    try {
      schemaManager.updateSchema(tableId.getBaseTableId(), topic);
    } catch (BigQueryException exception) {
      throw new BigQueryConnectException(
          "Failed to update table schema for: " + tableId.getBaseTableId(), exception);
    }
  }

  private void attemptTableCreate(TableId tableId, String topic) {
    try {
      schemaManager.createTable(tableId, topic);
      logger.info("Table {} does not exist, auto-created table for topic {}", tableId, topic);
    } catch (BigQueryException exception) {
      throw new BigQueryConnectException(
              "Failed to create table " + tableId, exception);
    }
  }
}
