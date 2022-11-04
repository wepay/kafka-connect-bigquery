package com.wepay.kafka.connect.bigquery;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import junit.framework.TestCase;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;

public class ErrantRecordsManagerTest extends TestCase {

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
    }

    public void testSendExceptionToDLQ_Basic() {
        Map<String, String> properties = new HashMap<>();
        properties.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "false");
        properties.put(BigQuerySinkConfig.TOPICS_CONFIG, "test");
        properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "test");
        properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "test");
        properties.put(BigQuerySinkTaskConfig.TASK_ID_CONFIG, "0");

        properties.put(BigQuerySinkConfig.ERRANT_RECORDS_REGEX_CONFIG, "yes");
        BigQuerySinkTaskConfig config = new BigQuerySinkTaskConfig(properties);

        ErrantRecordsManager recordChecker = new ErrantRecordsManager(config, null);

        Exception e = new Exception("yes");
        boolean sent = recordChecker.hasExceptionsToSendToDLQ(e);
        assertTrue(sent);

        e = new Exception("no");
        sent = recordChecker.hasExceptionsToSendToDLQ(e);
        assertFalse(sent);
    }

    public void testSendExceptionToDLQ_Choice() {
        Map<String, String> properties = new HashMap<>();
        properties.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "false");
        properties.put(BigQuerySinkConfig.TOPICS_CONFIG, "test");
        properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "test");
        properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "test");
        properties.put(BigQuerySinkTaskConfig.TASK_ID_CONFIG, "0");

        properties.put(BigQuerySinkConfig.ERRANT_RECORDS_REGEX_CONFIG, "yes|no");
        BigQuerySinkTaskConfig config = new BigQuerySinkTaskConfig(properties);

        ErrantRecordsManager recordChecker = new ErrantRecordsManager(config, null);

        Exception e = new Exception("yes");
        boolean sent = recordChecker.hasExceptionsToSendToDLQ(e);
        assertTrue(sent);

        e = new Exception("no");
        sent = recordChecker.hasExceptionsToSendToDLQ(e);
        assertTrue(sent);
    }

    public void testSendExceptionToDLQ_FullRegex() {
        Map<String, String> properties = new HashMap<>();
        properties.put(BigQuerySinkConfig.TABLE_CREATE_CONFIG, "false");
        properties.put(BigQuerySinkConfig.TOPICS_CONFIG, "test");
        properties.put(BigQuerySinkConfig.PROJECT_CONFIG, "test");
        properties.put(BigQuerySinkConfig.DEFAULT_DATASET_CONFIG, "test");
        properties.put(BigQuerySinkTaskConfig.TASK_ID_CONFIG, "0");

        properties.put(BigQuerySinkConfig.ERRANT_RECORDS_REGEX_CONFIG, "^[Ee]rror");
        BigQuerySinkTaskConfig config = new BigQuerySinkTaskConfig(properties);

        ErrantRecordsManager recordChecker = new ErrantRecordsManager(config, null);

        Exception e = new Exception("Error blah");
        boolean sent = recordChecker.hasExceptionsToSendToDLQ(e);
        assertTrue(sent);

        e = new Exception("Does not start with error");
        sent = recordChecker.hasExceptionsToSendToDLQ(e);
        assertFalse(sent);
    }
}
