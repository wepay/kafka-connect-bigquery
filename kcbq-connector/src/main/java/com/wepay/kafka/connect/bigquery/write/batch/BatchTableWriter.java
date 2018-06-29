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
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.gson.Gson;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class BatchTableWriter implements Runnable {
    private Gson gson;

    private BlobInfo blobInfo;
    private Storage storage;
    private String sourceUri;

    private BigQuery bigQuery;
    private TableId tableId;
    private Schema schema;
    private LoadJobConfiguration loadJobConfiguration;

    public BatchTableWriter(String bucketName, String blobName, Storage storage,
                            TableId tableId, BigQuery bigQuery, Schema schema) {
        gson = new Gson();

        BlobId blobId = BlobId.of(bucketName, blobName+".json");
        blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/json").build();
        this.storage = storage;
        this.sourceUri = "gs://" + bucketName + "/" + blobName + ".json";

        this.tableId = tableId;
        this.bigQuery = bigQuery;
        this.schema = schema;
        this.loadJobConfiguration =
                LoadJobConfiguration.builder(tableId, sourceUri)
                .setFormatOptions(FormatOptions.json())
                .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                .setSchema(schema)
                .build();
    }

    @Override
    public void run() {
        //todo implement
    }

    void transferBlobToBigQuery() throws InterruptedException, TimeoutException {
        Job loadJob = bigQuery.create(JobInfo.of(loadJobConfiguration));
        try {
            loadJob = loadJob.waitFor();
        } catch (InterruptedException ie) {
            throw new InterruptedException(ie + "Transfer from GCS blob to BigQuery unsuccessful.");
        } catch (TimeoutException toe) {
            throw new TimeoutException(toe + "Transfer from GCS blob to BigQuery unsuccessful.");
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
    String toJson(List<Map<String, Object>> records) {
        StringBuilder jsonRecordsBuilder = new StringBuilder("");
        for (Map<String, Object> record : records) {
            jsonRecordsBuilder.append(toJson(record));
            jsonRecordsBuilder.append("\n");
        }
        return jsonRecordsBuilder.toString();
    }
}
