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


import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.gson.Gson;

import org.apache.kafka.connect.errors.ConnectException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Batch Table Writer that uploads records to GCS as a blob
 * and then triggers a load job from that GCS file to BigQuery.
 */
public class GCSBatchTableWriter implements Runnable {
  private static final Gson gson = new Gson();
  private static final int NOT_FOUND_ERROR_CODE = 404;

  private final BlobInfo blobInfo;
  private final Storage storage;
  private final String sourceUri;

  private final BigQuery bigQuery;
  private final LoadJobConfiguration loadJobConfiguration;

  private final List<Map<String, Object>> records;

  /**
   * Initializes a batch table writer with a full list of records to write.
   * @param bucketName The name of the bucket to upload the blob in
   * @param blobName Full path within the bucket to the blob (without the extension)
   * @param storage GCS Storage
   * @param tableId {@link TableId} of the BigQuery table to upload to
   * @param bigQuery {@link BigQuery} Object used to perform upload
   * @param records Records to upload to BigQuery through GCS
   */
  public GCSBatchTableWriter(String bucketName,
                             String blobName,
                             Storage storage,
                             TableId tableId,
                             BigQuery bigQuery,
                             List<Map<String, Object>> records) {
    BlobId blobId = BlobId.of(bucketName, blobName + ".json");
    blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/json").build();
    this.storage = storage;
    this.sourceUri = String.format("gs://%s/%s.json", bucketName, blobName);

    this.bigQuery = bigQuery;

    // Check if the table specified exists
    // This error shouldn't be thrown. All tables should be created by the connector at startup
    if (bigQuery.getTable(tableId) == null || !bigQuery.getTable(tableId).exists()) {
      throw new ConnectException("Table with TableId %s does not exist.");
    }

    this.loadJobConfiguration =
        LoadJobConfiguration.builder(tableId, sourceUri)
                            .setFormatOptions(FormatOptions.json())
                            .setCreateDisposition(JobInfo.CreateDisposition.CREATE_NEVER)
                            .build();

    this.records = records;
  }

  @Override
  public void run() {
    //todo implement
  }

  /**
   * Triggers a load job to transfer JSON records from a GCS blob to a BigQuery table.
   */
  private void triggerBigQueryLoadJob() {
    Job loadJob = bigQuery.create(JobInfo.of(loadJobConfiguration));
    try {
      loadJob.waitFor();
    } catch (InterruptedException | TimeoutException exception) {
      String exceptionMessage = String.format("%s.Source URI = \"%s\" Table = \"%s\"",
          "Transfer from GCS blob to BigQuery unsuccessful.",
          loadJobConfiguration.getSourceUris(),
          loadJobConfiguration.getDestinationTable());
      throw new ConnectException(exceptionMessage, exception);
    }
  }

  /**
   * Creates a JSON string containing all records and uploads it as a blob to GCS
   * @return The blob uploaded to GCS
   */
  private Blob uploadRecordsToGcs() {
    try {
      return uploadBlobToGcs(new ByteArrayInputStream(toJson(records).getBytes("UTF-8")));
    } catch (UnsupportedEncodingException uee) {
      throw new ConnectException("Failed to upload blob to GCS", uee);
    }
  }

  private Blob uploadBlobToGcs(InputStream blobContent) {
    // todo look into creating from a string because this is depreciated - input stream cannot retry
    // todo consider if it would be worth it to switch to a resumable method of uploading
    return storage.create(blobInfo, blobContent);
  }

  private String toJson(Map<String, Object> record) {
    return gson.toJson(record);
  }

  private String toJson(List<Map<String, Object>> records) {
    StringBuilder jsonRecordsBuilder = new StringBuilder("");
    for (Map<String, Object> record : records) {
      jsonRecordsBuilder.append(toJson(record));
      jsonRecordsBuilder.append("\n");
    }
    return jsonRecordsBuilder.toString();
  }
}
