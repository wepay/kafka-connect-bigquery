package com.wepay.kafka.connect.bigquery.retrieve;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * Fetches the key Schema and value Schema from a Sink Record
 */

public class IdentitySchemaRetriever implements SchemaRetriever {

    @Override
    public void configure(Map<String, String> properties) {
    }

    @Override
    public Schema retrieveKeySchema(SinkRecord record) {
        return record.keySchema();
    }

    @Override
    public Schema retrieveValueSchema(SinkRecord record) {
        return record.valueSchema();
    }
}
