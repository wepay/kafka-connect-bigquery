package com.wepay.kafka.connect.bigquery;


import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LoadJobConfiguration;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.wepay.kafka.connect.bigquery.write.row.GCSToBQWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GCSToBQLoadTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(GCSToBQLoadTask.class);

    /*
     * Steps to be taken:
     * 1. get the list of all the objects in the GCS bucket (up to max file/size)
     * 2. run a load job with all those objects into the given table(s?)
     * 3. delete the loaded objects from the GCS bucket.
     */

    private final BigQuery bigQuery;
    private final Bucket bucket;
    private final Map<Job, List<Blob>> activeJobs;
    private final int waitIntervalMs;

    // these numbers are intended to try to make this task not excede Google Cloud Quotas.
    // see: https://cloud.google.com/bigquery/quotas#load_jobs
    private static int FILE_LOAD_LIMIT = 10000;
    // max number of files we can load in a single load job
    private static long MAX_LOAD_SIZE_B = 15 * 1000000000000L; // 15TB
    // max total size (in bytes) of the files we can load in a single load job.
    private static String SOURCE_URI_FORMAT = "gs://%s/%s";
    public static final Pattern METADATA_TABLE_PATTERN = Pattern.compile("(?<project>[^:]+):(?<dataset>[^.]+).(?<table>.+)");

    public GCSToBQLoadTask(BigQuery bigQuery, Bucket bucket, int waitIntervalMs) {
        this.bigQuery = bigQuery;
        this.bucket = bucket;
        this.activeJobs = new HashMap<>();
        this.waitIntervalMs = waitIntervalMs;
    }

    private Map<TableId, List<Blob>> getBlobsUpToLimit() {
        Map<TableId, List<Blob>> tableToURIs = new HashMap<>();
        //List<String> URIs = new ArrayList<>();
        Map<TableId, Long> tableToCurrentLoadSize = new HashMap<>();

        Page<Blob> list = bucket.list();
        for (Blob blob : list.iterateAll()) {
            TableId table = getTableFromBlob(blob);

            if (table == null) {
                // don't do anything
                continue;
            }

            if (!tableToURIs.containsKey(table)) {
                // initialize maps, if we haven't seen this table before.
                tableToURIs.put(table, new ArrayList<>());
                tableToCurrentLoadSize.put(table, 0L);
            }

            long newSize = tableToCurrentLoadSize.get(table) + blob.getSize();
            // if this file does not cause us to exceed our per-request quota limits...
            if (newSize <= MAX_LOAD_SIZE_B && tableToURIs.get(table).size() < FILE_LOAD_LIMIT) {
                // ...add the file (and update the load size)
                tableToURIs.get(table).add(blob);
                tableToCurrentLoadSize.put(table, newSize);
            }
        }
        return tableToURIs;
    }

    /**
     * Given a blob, return the {@link TableId} this blob should be inserted into.
     * @param blob the blob
     * @return the TableId this data should be loaded into, or null if we could not tell what table it should be loaded into.
     */
    private TableId getTableFromBlob(Blob blob) {

        if (blob.getMetadata() == null || blob.getMetadata().get(GCSToBQWriter.GCS_METADATA_TABLE_KEY) == null) {
            logger.error("Found blob {}\\{} with no metadata.", blob.getBucket(), blob.getName());
            return null;
        }

        String serializedTableId = blob.getMetadata().get(GCSToBQWriter.GCS_METADATA_TABLE_KEY);
        Matcher matcher = METADATA_TABLE_PATTERN.matcher(serializedTableId);
        String project = matcher.group("project");
        String dataset = matcher.group("dataset");
        String table = matcher.group("table");

        if (project == null || dataset == null || table == null) {
            logger.error("Found blob `{}\\{}` without parsable table metadata.", blob.getBucket(), blob.getName());
            return null;
        }

        return TableId.of(project, dataset, table);
    }

    private Map<Job, List<Blob>> triggerBigQueryLoadJobs(Map<TableId, List<Blob>> tablesToBlobs) {
        Map<Job, List<Blob>> newJobs = new HashMap<>(tablesToBlobs.size());
        for(Map.Entry<TableId, List<Blob>> entry : tablesToBlobs.entrySet()) {
            newJobs.put(triggerBigQueryLoadJob(entry.getKey(), entry.getValue()), entry.getValue());
        }
        return newJobs;
    }

    private Job triggerBigQueryLoadJob(TableId table, List<Blob> blobs) {
        List<String> URIs = blobsToURIs(blobs);
        // create job load configuration
        LoadJobConfiguration loadJobConfiguration =
            LoadJobConfiguration.newBuilder(table, URIs)
                .setFormatOptions(FormatOptions.json())
                .setCreateDisposition(JobInfo.CreateDisposition.CREATE_IF_NEEDED)
                .build();
        // create and return the job.
        return bigQuery.create(JobInfo.of(loadJobConfiguration));
    }

    private List<String> blobsToURIs(List<Blob> blobs) {
        List<String> URIs = new ArrayList<>(blobs.size());
        for(Blob blob: blobs) {
            URIs.add(String.format(SOURCE_URI_FORMAT, bucket.getName(), blob.getName()));
        }
        return URIs;
    }

    /**
     * check all active jobs. Remove those that have completed successfully and throw an exception containing those that have failed.
     * @throws ??
     */
    private void checkJobs() {
        Iterator<Job> jobIterator = activeJobs.keySet().iterator();
        while (jobIterator.hasNext()) {
            Job job = jobIterator.next();
            try {
                if (job.isDone()){
                    List<Blob> blobsToDelete = activeJobs.remove(job);
                    for (Blob blob : blobsToDelete) {
                        blob.delete();
                    }
                }
            } catch (BigQueryException ex) {
                // log a message.
                logger.error("GCS to BQ load job failed", ex);
                // remove job from active jobs (it's not active anymore)
                activeJobs.remove(job);
            }
        }
    }

    @Override
    public void run() {
        try {
            Thread.sleep(waitIntervalMs);
        } catch (InterruptedException e) {
            // todo
            e.printStackTrace();
        }
        Map<TableId, List<Blob>> tablesToSourceURIs = getBlobsUpToLimit();
        Map<Job, List<Blob>> jobListMap = triggerBigQueryLoadJobs(tablesToSourceURIs);
        activeJobs.putAll(jobListMap);
        checkJobs();
    }
}
