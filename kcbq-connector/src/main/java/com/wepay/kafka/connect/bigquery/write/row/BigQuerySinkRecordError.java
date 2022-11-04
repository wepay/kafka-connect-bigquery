package com.wepay.kafka.connect.bigquery.write.row;

import com.google.cloud.bigquery.BigQueryError;
import org.apache.kafka.connect.sink.SinkRecord;

public class BigQuerySinkRecordError {

    private final SinkRecord sinkRecord;
    private final BigQueryError bigQueryError;

    public BigQuerySinkRecordError(String reason, String location, String message, String debugInfo, SinkRecord sinkRecord) {
        this.sinkRecord = sinkRecord;
        this.bigQueryError = new BigQueryError(reason, location, message, debugInfo);
    }

    public BigQuerySinkRecordError(String reason, String location, String message, SinkRecord sinkRecord) {
        this.sinkRecord = sinkRecord;
        this.bigQueryError = new BigQueryError(reason, location, message);
    }
}
