package com.wepay.kafka.connect.bigquery;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Set;

public class ErrantRecordsContext {
    private final Set<SinkRecord> rows;
    private final Exception error;

    public ErrantRecordsContext(Set<SinkRecord> rows, Exception error) {
        this.rows = rows;
        this.error = error;
    }

    public Set<SinkRecord> getRows() {
        return rows;
    }

    public Exception getError() {
        return error;
    }
}
