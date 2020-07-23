package com.wepay.kafka.connect.bigquery.utils;

import com.google.cloud.bigquery.InsertAllRequest;
import com.wepay.kafka.connect.bigquery.api.KafkaSchemaRecordType;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;
import com.wepay.kafka.connect.bigquery.convert.KafkaDataBuilder;
import com.wepay.kafka.connect.bigquery.convert.RecordConverter;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;
import java.util.Optional;

/**
 * A class for converting a {@link SinkRecord SinkRecord} to {@link InsertAllRequest.RowToInsert BigQuery row}
 */

public class SinkRecordConverter {
    private final BigQuerySinkTaskConfig config;
    private final RecordConverter<Map<String, Object>> recordConverter;

    public SinkRecordConverter(BigQuerySinkTaskConfig config) {
        this.config = config;
        this.recordConverter = config.getRecordConverter();
    }
    public InsertAllRequest.RowToInsert getRecordRow(SinkRecord record) {
        Map<String, Object> convertedRecord = recordConverter.convertRecord(record, KafkaSchemaRecordType.VALUE);
        Optional<String> kafkaKeyFieldName = config.getKafkaKeyFieldName();
        if (kafkaKeyFieldName.isPresent()) {
            convertedRecord.put(kafkaKeyFieldName.get(), recordConverter.convertRecord(record, KafkaSchemaRecordType.KEY));
        }
        Optional<String> kafkaDataFieldName = config.getKafkaDataFieldName();
        if (kafkaDataFieldName.isPresent()) {
            convertedRecord.put(kafkaDataFieldName.get(), KafkaDataBuilder.buildKafkaDataRecord(record));
        }
        if (config.getBoolean(config.SANITIZE_FIELD_NAME_CONFIG)) {
            convertedRecord = FieldNameSanitizer.replaceInvalidKeys(convertedRecord);
        }
        return InsertAllRequest.RowToInsert.of(getRowId(record), convertedRecord);
    }

    private String getRowId(SinkRecord record) {
        return String.format("%s-%d-%d",
                record.topic(),
                record.kafkaPartition(),
                record.kafkaOffset());
    }
}
