/*
 * Copyright 2022 Confluent, Inc.
 *
 * This software contains code derived from the WePay BigQuery Kafka Connector, Copyright WePay, Inc.
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

package com.wepay.kafka.connect.bigquery;

import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrantRecordsManager {
    private final boolean enabled;
    private Pattern regexPattern;
    private ErrantRecordReporter errantRecordReporter;

    private static final Logger logger = LoggerFactory.getLogger(ErrantRecordsManager.class);

    public ErrantRecordsManager(BigQuerySinkTaskConfig config, ErrantRecordReporter errantRecordReporter) {
        String regex = config.getString(BigQuerySinkTaskConfig.ERRANT_RECORDS_REGEX_CONFIG);
        this.enabled = regex != null;
        if (enabled) {
            this.regexPattern = Pattern.compile(regex);
        }
        this.errantRecordReporter = errantRecordReporter;
    }

    public boolean hasExceptionsToSendToDLQ(Exception e) {
        if (!enabled) { return false; }
        Matcher matcher = regexPattern.matcher(e.getMessage());
        return matcher.find();
    }

    public boolean isEnabled() {
        return enabled;
    }

    public void sendRowsToDLQ(Set<SinkRecord> rows, Exception e) {
        for (SinkRecord r : rows) {
            errantRecordReporter.report(r, e); // TODO return a Future and wait for all of them?
        }
        logger.info("Sending {} records to the DLQ (as requested in the config by {}).",
                rows.size(),
                BigQuerySinkTaskConfig.ERRANT_RECORDS_REGEX_CONFIG);
    }
}
