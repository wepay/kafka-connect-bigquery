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
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.gson.Gson;

import com.wepay.kafka.connect.bigquery.exception.GCSConnectException;

import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * A class for batch writing list of rows to BigQuery through GCS
 */
public class GCSToBQWriter {
  private static final Logger logger = LoggerFactory.getLogger(GCSToBQWriter.class);

  private static Gson gson = new Gson();

  private final Storage storage;

  private final BigQuery bigQuery;
  private LoadJobConfiguration loadJobConfiguration;

  private static final int WAIT_MAX_JITTER = 1000;

  private static final Random random = new Random();

  private int retries;
  private long retryWaitMs;

  /**
   * Initializes a batch GCS writer with a full list of rows to write.
   * @param storage GCS Storage
   * @param bigQuery {@link BigQuery} Object used to perform upload
   */
  public GCSToBQWriter(Storage storage,
                       BigQuery bigQuery,
                       int retries,
                       long retryWaitMs) {
    this.storage = storage;
    this.bigQuery = bigQuery;

    this.retries = retries;
    this.retryWaitMs = retryWaitMs;
  }

  public void writeRows(List<RowToInsert> rows,
                        TableId tableId,
                        String bucketName,
                        String blobName)
      throws InterruptedException {

    // Get Source URI
    BlobId blobId = BlobId.of(bucketName, blobName);
    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/json").build();
    String sourceUri = String.format("gs://%s/%s", bucketName, blobName);

    // Check if the table specified exists
    // This error shouldn't be thrown. All tables should be created by the connector at startup
    if (bigQuery.getTable(tableId) == null) {
      throw new ConnectException(
          String.format("Table with TableId %s does not exist.", tableId.getTable()));
    }

    // Create a job configuration
    this.loadJobConfiguration =
        LoadJobConfiguration.builder(tableId, sourceUri)
            .setFormatOptions(FormatOptions.json())
            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
            .build();

    int retryCount = 0;
    boolean exceptionsOccurred;
    do {
      if (retryCount > 0) {
        waitRandomTime();
      }
      exceptionsOccurred = false;
      // Perform GCS Upload and BQ Transfer
      try {
        uploadRowsToGcs(rows, blobInfo);
        triggerBigQueryLoadJob();
      } catch (ConnectException ce) {
        exceptionsOccurred = true;
        logger.warn("Exceptions occurred for table {}, attempting retry", tableId.getTable());
      }
      retryCount++;
    } while (exceptionsOccurred && (retryCount < retries));

    logger.info("Batch loaded {} rows", rows.size());
  }

  /**
   * Triggers a load job to transfer JSON records from a GCS blob to a BigQuery table.
   */
  private void triggerBigQueryLoadJob() {
    bigQuery.create(JobInfo.of(loadJobConfiguration));
  }

  /**
   * Creates a JSON string containing all records and uploads it as a blob to GCS
   * @return The blob uploaded to GCS
   */
  private Blob uploadRowsToGcs(List<RowToInsert> rows, BlobInfo blobInfo) {
    try {
      Blob resultBlob = uploadBlobToGcs(
          new ByteArrayInputStream(toJson(rows).getBytes("UTF-8")), blobInfo);
      return resultBlob;
    } catch (UnsupportedEncodingException uee) {
      throw new GCSConnectException("Failed to upload blob to GCS", uee);
    }
  }

  private Blob uploadBlobToGcs(InputStream blobContent, BlobInfo blobInfo) {
    // todo look into creating from a string because this is depreciated - input stream cannot retry
    // todo consider if it would be worth it to switch to a resumable method of uploading
    return storage.create(blobInfo, blobContent);
  }

  /**
   * Converts a list of rows to a serialized JSON string of records
   * @param rows rows to be serialized
   * @return The resulting newline delimited JSON string containing all records in the original list
   */
  private String toJson(List<RowToInsert> rows) {
    StringBuilder jsonRecordsBuilder = new StringBuilder("");
    for (RowToInsert row : rows) {
      Map<String, Object> record = row.getContent();
      jsonRecordsBuilder.append(gson.toJson(record));
      jsonRecordsBuilder.append("\n");
    }
    return jsonRecordsBuilder.toString();
  }

  /**
   * Wait at least {@link #retryWaitMs}, with up to an additional 1 second of random jitter.
   * @throws InterruptedException if interrupted.
   */
  private void waitRandomTime() throws InterruptedException {
    Thread.sleep(retryWaitMs + random.nextInt(WAIT_MAX_JITTER));
  }
}
