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
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllRequest;
import com.google.cloud.bigquery.InsertAllResponse;

import com.wepay.kafka.connect.bigquery.SchemaManager;
import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.connect.data.Schema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link BigQueryWriter} capable of updating BigQuery table schemas.
 */
public class AdaptiveBigQueryWriter extends BigQueryWriter {
  private static final Logger logger = LoggerFactory.getLogger(AdaptiveBigQueryWriter.class);

  private final BigQuery bigQuery;
  private final SchemaManager schemaManager;

  /**
   * @param bigQuery Used to send write requests to BigQuery.
   * @param schemaManager Used to update BigQuery tables.
   * @param retry How many retries to make in the event of a 500/503 error.
   * @param retryWait How long to wait in between retries.
   * @param metrics Metrics for kcbq.
   */
  public AdaptiveBigQueryWriter(BigQuery bigQuery,
                                SchemaManager schemaManager,
                                int retry,
                                long retryWait,
                                Metrics metrics) {
    super(retry, retryWait, metrics);
    this.bigQuery = bigQuery;
    this.schemaManager = schemaManager;
  }

  /**
   * Sends the request to BigQuery, then checks the response to see if any errors have occurred. If
   * any have, and all errors can be blamed upon invalid columns in the rows sent, attempts to
   * update the schema of the table in BigQuery and then performs the same write request.
   * @param request The request to send to BigQuery.
   * @param topic The Kafka topic that the row data came from.
   * @param schemas The unique Schemas for the row data.
   */
  @Override
  public void performWriteRequest(InsertAllRequest request, String topic, Set<Schema> schemas) {
    InsertAllResponse writeResponse = bigQuery.insertAll(request);
    // Should only perform one schema update attempt; may have to continue insert attempts due to
    // BigQuery schema updates taking up to two minutes to take effect
    if (writeResponse.hasErrors()
        && onlyContainsInvalidSchemaErrors(writeResponse.insertErrors())) {
      schemaManager.updateSchema(request.table(), topic, schemas);
    }

    // Schema update might be delayed, so multiple insertion attempts may be necessary
    while (writeResponse.hasErrors()) {
      logger.trace("insertion failed");
      if (onlyContainsInvalidSchemaErrors(writeResponse.insertErrors())) {
        logger.debug("re-attempting insertion");
        writeResponse = bigQuery.insertAll(request);
      } else {
        throw new BigQueryConnectException(writeResponse.insertErrors());
      }
    }
    logger.debug("table insertion completed successfully");
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
    for (List<BigQueryError> errorList : errors.values()) {
      for (BigQueryError error : errorList) {
        if (!(error.reason().equals("invalid") && error.message().contains("no such field"))) {
          return false;
        }
      }
    }
    return true;
  }
}
