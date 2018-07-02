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
import com.google.cloud.bigquery.BigQueryException;
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
import jdk.nashorn.internal.runtime.regexp.joni.exception.ValueException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Batch Table Writer that uploads records to GCS as a blob and then uploads the contents of that blob to BigQuery
 */
public class BatchTableWriter implements Runnable {
    private static final Gson gson = new Gson();
    private static final int NOT_FOUND_ERROR_CODE = 404;

    private final BlobInfo blobInfo;
    private final Storage storage;
    private final String sourceUri;

    private final BigQuery bigQuery;
    private final LoadJobConfiguration loadJobConfiguration;

    private final List<Map<String, Object>> records;

    /**
     * @param bucketName The name of the bucket to which a blob containing record information should be uploaded
     * @param blobName Full path within the bucket to the blob (without the extension) (Doesn't need to be pre-created)
     * @param storage GCS Storage
     * @param tableId {@link TableId} of the BigQuery table to upload to
     * @param bigQuery {@link BigQuery} Object used to perform upload
     * @param records Records to upload to BigQuery through GCS
     */
    public BatchTableWriter(String bucketName, String blobName, Storage storage,
                            TableId tableId, BigQuery bigQuery,
                            List<Map<String, Object>> records) {
        BlobId blobId = BlobId.of(bucketName, blobName+".json");
        blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/json").build();
        this.storage = storage;
        this.sourceUri = String.format("gs://%s/%s.json", bucketName, blobName);

        this.bigQuery = bigQuery;

        // Check if the table specified exists
        try {
            if (!bigQuery.getTable(tableId).exists()) {
                throw new BigQueryException(NOT_FOUND_ERROR_CODE, "");
            }
        } catch (BigQueryException | NullPointerException exception) {
            throw new BigQueryException(NOT_FOUND_ERROR_CODE,
                    exception + String.format("Table id for table %s does not exist", tableId.getTable()));
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

    private void transferBlobToBigQuery() throws RuntimeException {
        Job loadJob = bigQuery.create(JobInfo.of(loadJobConfiguration));
        String exceptionMessage = String.format("%s.\nSource URI = \"%s\"\nTable = \"%s\"",
                "Transfer from GCS blob to BigQuery unsuccessful.",
                loadJobConfiguration.getSourceUris(),
                loadJobConfiguration.getDestinationTable());
        try {
            loadJob = loadJob.waitFor();
        } catch (InterruptedException | TimeoutException exception) {
            throw new RuntimeException(exceptionMessage, exception);
        }
    }

    public Blob uploadRecords(List<Map<String, Object>> records) {
        return uploadBlob(new ByteArrayInputStream(toJson(records).getBytes()));
    }

    private Blob uploadBlob(InputStream jsonRecords) {
        // todo look into creating from a string because this is depreciated - input stream cannot retry
        // todo consider if it would be worth it to switch to a resumable method of uploading
        return storage.create(blobInfo, jsonRecords);
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
