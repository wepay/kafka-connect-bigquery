package com.wepay.kafka.connect.bigquery.it.utils;

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


import com.google.cloud.bigquery.*;
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;
import com.google.cloud.storage.*;
import com.google.gson.Gson;
import com.wepay.kafka.connect.bigquery.BigQueryHelper;
import com.wepay.kafka.connect.bigquery.GCSBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.wepay.kafka.connect.bigquery.write.row.GCSToBQWriter.GCS_METADATA_TABLE_KEY;

public class BucketClearer {

    private static final Logger logger = LoggerFactory.getLogger(BucketClearer.class);

    /**
     * Clears tables in the given project and dataset, using a provided JSON service account key.
     */
    public static void main(String[] args) {
        if (args.length < 6) {
            usage();
        }

        String keyfile = args[0];
        String project = args[1];
        String bucketName = args[2];
        String keySource = args[3];
        String folder = args[4];
        String dataset = args[5];

        Storage gcs = new GCSBuilder(project).setKey(keyfile).setKeySource(keySource).build();
        Bucket bucket = getBucket(bucketName, gcs);

        // Empty the bucket
        logger.info("Emptying bucket");
        for (Blob blob : bucket.list().iterateAll()) {
            gcs.delete(blob.getBlobId());
        }

        // If using a folder, add some dummy data to an adjacent folder to ensure this doesn't get loaded
        if (folder != null && !folder.equals("")) {
            BlobInfo blobInAdjacentFolderInfo = getBlobInfo(project, dataset, keySource, keyfile, bucketName);
            byte[] blobInAdjacentFolderContent = getBlobContent();
            gcs.create(blobInAdjacentFolderInfo, blobInAdjacentFolderContent);
        }
    }

    private static Bucket getBucket(String bucketName, Storage gcs) {
        Bucket bucket = gcs.get(bucketName);
        if (bucket == null) {
            BucketInfo bucketInfo = BucketInfo.of(bucketName);
            bucket = gcs.create(bucketInfo);
        }
        return bucket;
    }

    private static BlobInfo getBlobInfo(String project, String dataset, String keySource, String keyfile, String bucketName) {
        BlobId blobInAdjacentFolderId = BlobId.of(bucketName, "adjacentFolder/dataThatShouldNotLoad");

        // create bq table
        TableId tableId = TableId.of(project, dataset, "kcbq_test_shouldnt_populate");

        Field[] fields = {
                Field.of("dummy_field", LegacySQLTypeName.STRING)
        };
        TableInfo tableInfo = TableInfo.newBuilder(tableId, StandardTableDefinition.of(Schema.of(fields))).build();

        BigQuery bq = new BigQueryHelper().setKeySource(keySource).connect(project, keyfile);
        bq.create(tableInfo);
        // ---------------------

        Map<String, String> metadata = getMetadata(tableId);

        return BlobInfo.newBuilder(blobInAdjacentFolderId).setContentType("text/json").setMetadata(metadata).build();
    }

    private static Map<String, String> getMetadata(TableId tableId) {
        StringBuilder sb = new StringBuilder();
        if (tableId.getProject() != null) {
            sb.append(tableId.getProject()).append(":");
        }
        String serializedTableId =
                sb.append(tableId.getDataset()).append(".").append(tableId.getTable()).toString();

        return Collections.singletonMap(GCS_METADATA_TABLE_KEY, serializedTableId);
    }

    private static byte[] getBlobContent() {
        Map<String, String> content = new HashMap<>();
        content.put("dummy_field", "dummy_value");
        List<RowToInsert> blobInAdjacentFolderContent = new ArrayList<>();
        blobInAdjacentFolderContent.add(RowToInsert.of(UUID.randomUUID().toString(), content));
        return toJson(blobInAdjacentFolderContent).getBytes(StandardCharsets.UTF_8);
    }

    private static String toJson(List<RowToInsert> rows) {
        Gson gson = new Gson();

        StringBuilder jsonRecordsBuilder = new StringBuilder();
        for (RowToInsert row : rows) {
            Map<String, Object> record = row.getContent();
            jsonRecordsBuilder.append(gson.toJson(record));
            jsonRecordsBuilder.append("\n");
        }
        return jsonRecordsBuilder.toString();
    }

    private static void usage() {
        System.err.println(
                "usage: BucketClearer <key_file> <project_name> <key_source> <bucket_name>"
        );
        System.exit(1);
    }
}
