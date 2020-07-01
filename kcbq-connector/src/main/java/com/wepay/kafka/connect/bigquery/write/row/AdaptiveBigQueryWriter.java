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
              && onlyContainsInvalidSchemaErrors(writeResponse.getInsertErrors()) && autoUpdateSchemas) {
        attemptSchemaUpdate(tableId, topic);
      }
    } catch (BigQueryException exception) {
      // Should only perform one table creation attempt.
      if (isTableNotExistedException(exception) && autoCreateTables ) {
        //In some cases the table exists but the partition still is not created,
        // so we should no throw an exception but wait
        try {
          if (bigQuery.getTable(tableId.getBaseTableId()) == null) {
            attemptTableCreate(tableId.getBaseTableId(), topic);
          }
        } catch(BigQueryConnectException ex) {
          BigQueryException cause = (BigQueryException)ex.getCause();
          //Ignore the exception because the table already exists
          if ( !(cause.getCode() == 409 && cause.getReason().equalsIgnoreCase("duplicate"))) {
            throw ex;
          }
        }
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
          // When autoCreateTable is used with autoUpdateSchemas and the MemorySchemaRetriever
          // a table with no schema can appear and is necessary to check again
          if (isTableMissingSchema(exception) && autoUpdateSchemas) {
            attemptSchemaUpdate(tableId, topic);
          }
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
      if (writeResponse == null || writeResponse.hasErrors()) {
        try {
          Thread.sleep(RETRY_WAIT_TIME);
        } catch (InterruptedException e) {
          // no-op, we want to keep retrying the insert
        }
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

  /*
   * Currently, the only way to determine the cause of an insert all failure is by examining the map
   * object returned by the insertErrors() method of an insert all response. The only way to
   * determine the cause of each individual error is by manually examining each error's reason() and
   * message() strings, and guessing what they mean. Ultimately, the goal of this method is to
   * return whether or not an insertion failed due solely to a mismatch between the schemas of the
   * inserted rows and the schema of the actual BigQuery table.
   * This is why we can't have nice things, Google.
   */
  private boolean onlyContainsInvalidSchemaErrors(Map<Long, List<BigQueryError>> errors) {
    boolean invalidSchemaError = false;
    for (List<BigQueryError> errorList : errors.values()) {
      for (BigQueryError error : errorList) {
        if (error.getReason().equals("invalid") && error.getMessage().contains("no such field")) {
          invalidSchemaError = true;
        } else if (!error.getReason().equals("stopped")) {
          /* if some rows are in the old schema format, and others aren't, the old schema
           * formatted rows will show up as error: stopped. We still want to continue if this is
           * the case, because these errors don't represent a unique error if there are also
           * invalidSchemaErrors.
           */
          return false;
        }
      }
    }
    // if we only saw "stopped" errors, we want to return false. (otherwise, return true)
    return invalidSchemaError;
  }
}
