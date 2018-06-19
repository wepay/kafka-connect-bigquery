package com.wepay.kafka.connect.bigquery.convert;

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


import com.google.cloud.bigquery.InsertAllRequest.RowToInsert;

import com.wepay.kafka.connect.bigquery.config.BigQuerySinkConfig;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.DebeziumLogicalConverters;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.KafkaLogicalConverters;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.LogicalConverterRegistry;
import com.wepay.kafka.connect.bigquery.convert.logicaltype.LogicalTypeConverter;
import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Class for converting from {@link SinkRecord SinkRecords} and BigQuery rows, which are represented
 * as {@link Map Maps} from {@link String Strings} to {@link Object Objects}.
 */
public class BigQueryRecordConverter implements RecordConverter<Map<String, Object>> {

  private boolean shouldConvertSpecialDouble;

  static {
    // force registration
    new DebeziumLogicalConverters();
    new KafkaLogicalConverters();
  }

  public BigQueryRecordConverter(boolean shouldConvertDoubleSpecial) {
    this.shouldConvertSpecialDouble = shouldConvertDoubleSpecial;
  }

  /**
   * Convert a {@link SinkRecord} into the contents of a BigQuery {@link RowToInsert}.
   *
   * @param kafkaConnectRecord The Kafka Connect record to convert. Must be of type {@link Struct},
   *                           in order to translate into a row format that requires each field to
   *                           consist of both a name and a value.
   * @return The result BigQuery row content.
   */
  public Map<String, Object> convertRecord(SinkRecord kafkaConnectRecord) {
    Schema kafkaConnectSchema = kafkaConnectRecord.valueSchema();
    if (kafkaConnectSchema.type() != Schema.Type.STRUCT) {
      throw new
          ConversionConnectException("Top-level Kafka Connect schema must be of type 'struct'");
    }
    return convertStruct(kafkaConnectRecord.value(), kafkaConnectSchema);
  }

  @SuppressWarnings("unchecked")
  private Object convertObject(Object kafkaConnectObject, Schema kafkaConnectSchema) {
    if (kafkaConnectObject == null) {
      if (kafkaConnectSchema.isOptional()) {
        // short circuit converting the object
        return null;
      } else {
        throw new ConversionConnectException(
            kafkaConnectSchema.name() + " is not optional, but converting object had null value");
      }
    }
    if (LogicalConverterRegistry.isRegisteredLogicalType(kafkaConnectSchema.name())) {
      return convertLogical(kafkaConnectObject, kafkaConnectSchema);
    }
    Schema.Type kafkaConnectSchemaType = kafkaConnectSchema.type();
    switch (kafkaConnectSchemaType) {
      case ARRAY:
        return convertArray(kafkaConnectObject, kafkaConnectSchema);
      case MAP:
        return convertMap(kafkaConnectObject, kafkaConnectSchema);
      case STRUCT:
        return convertStruct(kafkaConnectObject, kafkaConnectSchema);
      case BYTES:
        ByteBuffer byteBuffer = (ByteBuffer) kafkaConnectObject;
        byte[] bytes = byteBuffer.array();
        return Base64.getEncoder().encodeToString(bytes);
      case BOOLEAN:
        return (Boolean) kafkaConnectObject;
      case FLOAT32:
        return (Float) kafkaConnectObject;
      case FLOAT64:
        return convertDouble((Double)kafkaConnectObject);
      case INT8:
        return (Byte) kafkaConnectObject;
      case INT16:
        return (Short) kafkaConnectObject;
      case INT32:
        return (Integer) kafkaConnectObject;
      case INT64:
        return (Long) kafkaConnectObject;
      case STRING:
        return (String) kafkaConnectObject;
      default:
        throw new ConversionConnectException("Unrecognized schema type: " + kafkaConnectSchemaType);
    }
  }

  private Map<String, Object> convertStruct(Object kafkaConnectObject,
                                            Schema kafkaConnectSchema) {
    Map<String, Object> bigQueryRecord = new HashMap<>();
    List<Field> kafkaConnectSchemaFields = kafkaConnectSchema.fields();
    Struct kafkaConnectStruct = (Struct) kafkaConnectObject;
    for (Field kafkaConnectField : kafkaConnectSchemaFields) {
      Object bigQueryObject = convertObject(
          kafkaConnectStruct.get(kafkaConnectField.name()),
          kafkaConnectField.schema()
      );
      if (bigQueryObject != null) {
        bigQueryRecord.put(kafkaConnectField.name(), bigQueryObject);
      }
    }
    return bigQueryRecord;
  }

  @SuppressWarnings("unchecked")
  private List<Object> convertArray(Object kafkaConnectObject,
                                    Schema kafkaConnectSchema) {
    Schema kafkaConnectValueSchema = kafkaConnectSchema.valueSchema();
    List<Object> bigQueryList = new ArrayList<>();
    List<Object> kafkaConnectList = (List<Object>) kafkaConnectObject;
    for (Object kafkaConnectElement : kafkaConnectList) {
      Object bigQueryValue = convertObject(kafkaConnectElement, kafkaConnectValueSchema);
      bigQueryList.add(bigQueryValue);
    }
    return bigQueryList;
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> convertMap(Object kafkaConnectObject,
                                               Schema kafkaConnectSchema) {
    Schema kafkaConnectKeySchema = kafkaConnectSchema.keySchema();
    Schema kafkaConnectValueSchema = kafkaConnectSchema.valueSchema();
    List<Map<String, Object>> bigQueryEntryList = new ArrayList<>();
    Map<Object, Object> kafkaConnectMap = (Map<Object, Object>) kafkaConnectObject;
    for (Map.Entry kafkaConnectMapEntry : kafkaConnectMap.entrySet()) {
      Map<String, Object> bigQueryEntry = new HashMap<>();
      Object bigQueryKey = convertObject(
          kafkaConnectMapEntry.getKey(),
          kafkaConnectKeySchema
      );
      Object bigQueryValue = convertObject(
          kafkaConnectMapEntry.getValue(),
          kafkaConnectValueSchema
      );
      bigQueryEntry.put(BigQuerySchemaConverter.MAP_KEY_FIELD_NAME, bigQueryKey);
      bigQueryEntry.put(BigQuerySchemaConverter.MAP_VALUE_FIELD_NAME, bigQueryValue);
      bigQueryEntryList.add(bigQueryEntry);
    }
    return bigQueryEntryList;
  }

  private Object convertLogical(Object kafkaConnectObject,
                                Schema kafkaConnectSchema) {
    LogicalTypeConverter converter =
        LogicalConverterRegistry.getConverter(kafkaConnectSchema.name());
    return converter.convert(kafkaConnectObject);
  }

  /**
   * Converts a kafka connect {@link Double} into a value that can be stored into BigQuery
   * If this.shouldDonvertSpecialDouble is true, special values are converted as follows:
   * Double.POSITIVE_INFINITY -> Double.MAX_VALUE
   * Doulbe.NEGATIVE_INFINITY -> Double.MIN_VALUE
   * Double.NaN               -> Double.MIN_VALUE
   *
   * @param kafkaConnectDouble The Kafka Connect value to convert.
   *
   * @return The resulting Double value to put in BigQuery.
   */
  private Double convertDouble(Double kafkaConnectDouble) {
    if (shouldConvertSpecialDouble) {
      if (kafkaConnectDouble.equals(Double.POSITIVE_INFINITY)) {
        return Double.MAX_VALUE;
      } else if (kafkaConnectDouble.equals(Double.NEGATIVE_INFINITY)
              || Double.isNaN(kafkaConnectDouble)) {
        return Double.MIN_VALUE;
      }
    }
    return kafkaConnectDouble;
  }
}
