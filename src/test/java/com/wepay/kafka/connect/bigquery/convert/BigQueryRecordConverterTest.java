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


import static org.junit.Assert.assertEquals;

import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;

import org.apache.kafka.connect.sink.SinkRecord;

import org.junit.Test;

import java.nio.ByteBuffer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BigQueryRecordConverterTest {
  @Test(expected = ConversionConnectException.class)
  public void testTopLevelRecord() {
    SinkRecord kafkaConnectRecord = spoofSinkRecord(Schema.BOOLEAN_SCHEMA, false);
    new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
  }

  @Test
  public void testBoolean() {
    final String fieldName = "Boolean";
    final Boolean fieldValue = true;

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldValue);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.BOOLEAN_SCHEMA)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValue);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

    Map<String, Object> bigQueryTestRecord =
        new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testInteger() {
    final String fieldName = "Integer";
    final Byte fieldByteValue = (byte) 42;

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldByteValue);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT8_SCHEMA)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldByteValue);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

    Map<String, Object> bigQueryTestRecord =
        new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);

    final Short fieldShortValue = (short) 4242;
    bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldShortValue);

    kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT16_SCHEMA)
        .build();

    kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldShortValue);
    kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

    bigQueryTestRecord = new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);

    final Integer fieldIntegerValue = 424242;
    bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldIntegerValue);

    kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT32_SCHEMA)
        .build();

    kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldIntegerValue);
    kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

    bigQueryTestRecord = new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);

    final Long fieldLongValue = 424242424242L;
    bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldLongValue);

    kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.INT64_SCHEMA)
        .build();

    kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldLongValue);
    kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

    bigQueryTestRecord = new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test public void testFloat() {
    final String fieldName = "Float";
    final Float fieldFloatValue = 4242424242.4242F;

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldFloatValue);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.FLOAT32_SCHEMA)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldFloatValue);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

    Map<String, Object> bigQueryTestRecord =
        new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);

    final Double fieldDoubleValue = 4242424242.4242;

    bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldDoubleValue);

    kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.FLOAT64_SCHEMA)
        .build();

    kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldDoubleValue);
    kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

    bigQueryTestRecord = new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testString() {
    final String fieldName = "String";
    final String fieldValue = "42424242424242424242424242424242";

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldValue);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.STRING_SCHEMA)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValue);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

    Map<String, Object> bigQueryTestRecord =
        new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testStruct() {
    final String middleFieldStructName = "MiddleStruct";
    final String middleFieldArrayName = "MiddleArray";
    final String innerFieldStructName = "InnerStruct";
    final String innerFieldStringName = "InnerString";
    final String innerFieldIntegerName = "InnerInt";
    final String innerStringValue = "42";
    final Integer innerIntegerValue = 42;
    final List<Float> middleArrayValue = Arrays.asList(42.0f, 42.4f, 42.42f, 42.424f, 42.4242f);

    Map<String, Object> bigQueryExpectedInnerRecord = new HashMap<>();
    bigQueryExpectedInnerRecord.put(innerFieldStringName, innerStringValue);
    bigQueryExpectedInnerRecord.put(innerFieldIntegerName, innerIntegerValue);

    Schema kafkaConnectInnerSchema = SchemaBuilder
        .struct()
        .field(innerFieldStringName, Schema.STRING_SCHEMA)
        .field(innerFieldIntegerName, Schema.INT32_SCHEMA)
        .build();

    Struct kafkaConnectInnerStruct = new Struct(kafkaConnectInnerSchema);
    kafkaConnectInnerStruct.put(innerFieldStringName, innerStringValue);
    kafkaConnectInnerStruct.put(innerFieldIntegerName, innerIntegerValue);

    SinkRecord kafkaConnectInnerSinkRecord =
        spoofSinkRecord(kafkaConnectInnerSchema, kafkaConnectInnerStruct);
    Map<String, Object> bigQueryTestInnerRecord =
        new BigQueryRecordConverter().convertRecord(kafkaConnectInnerSinkRecord);
    assertEquals(bigQueryExpectedInnerRecord, bigQueryTestInnerRecord);


    Map<String, Object> bigQueryExpectedMiddleRecord = new HashMap<>();
    bigQueryExpectedMiddleRecord.put(innerFieldStructName, bigQueryTestInnerRecord);
    bigQueryExpectedMiddleRecord.put(middleFieldArrayName, middleArrayValue);

    Schema kafkaConnectMiddleSchema = SchemaBuilder
        .struct()
        .field(innerFieldStructName, kafkaConnectInnerSchema)
        .field(middleFieldArrayName, SchemaBuilder.array(Schema.FLOAT32_SCHEMA).build())
        .build();

    Struct kafkaConnectMiddleStruct = new Struct(kafkaConnectMiddleSchema);
    kafkaConnectMiddleStruct.put(innerFieldStructName, kafkaConnectInnerStruct);
    kafkaConnectMiddleStruct.put(middleFieldArrayName, middleArrayValue);

    SinkRecord kafkaConnectMiddleSinkRecord =
        spoofSinkRecord(kafkaConnectMiddleSchema, kafkaConnectMiddleStruct);
    Map<String, Object> bigQueryTestMiddleRecord =
        new BigQueryRecordConverter().convertRecord(kafkaConnectMiddleSinkRecord);
    assertEquals(bigQueryExpectedMiddleRecord, bigQueryTestMiddleRecord);


    Map<String, Object> bigQueryExpectedOuterRecord = new HashMap<>();
    bigQueryExpectedOuterRecord.put(innerFieldStructName, bigQueryTestInnerRecord);
    bigQueryExpectedOuterRecord.put(middleFieldStructName, bigQueryTestMiddleRecord);

    Schema kafkaConnectOuterSchema = SchemaBuilder
        .struct()
        .field(innerFieldStructName, kafkaConnectInnerSchema)
        .field(middleFieldStructName, kafkaConnectMiddleSchema)
        .build();

    Struct kafkaConnectOuterStruct = new Struct(kafkaConnectOuterSchema);
    kafkaConnectOuterStruct.put(innerFieldStructName, kafkaConnectInnerStruct);
    kafkaConnectOuterStruct.put(middleFieldStructName, kafkaConnectMiddleStruct);

    SinkRecord kafkaConnectOuterSinkRecord =
        spoofSinkRecord(kafkaConnectOuterSchema, kafkaConnectOuterStruct);
    Map<String, Object> bigQueryTestOuterRecord =
        new BigQueryRecordConverter().convertRecord(kafkaConnectOuterSinkRecord);
    assertEquals(bigQueryExpectedOuterRecord, bigQueryTestOuterRecord);
  }

  @Test
  public void testMap() {
    final String fieldName = "StringIntegerMap";
    final Map<Integer, Boolean> fieldValueKafkaConnect = new HashMap<>();
    final List<Map<String, Object>> fieldValueBigQuery = new ArrayList<>();

    for (int n = 2; n <= 10; n++) {
      boolean isPrime = true;
      for (int d : fieldValueKafkaConnect.keySet()) {
        if (n % d == 0) {
          isPrime = false;
          break;
        }
      }
      fieldValueKafkaConnect.put(n, isPrime);
      Map<String, Object> entryBigQuery = new HashMap<>();
      entryBigQuery.put(BigQuerySchemaConverter.MAP_KEY_FIELD_NAME, n);
      entryBigQuery.put(BigQuerySchemaConverter.MAP_VALUE_FIELD_NAME, isPrime);
      fieldValueBigQuery.add(entryBigQuery);
    }

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldValueBigQuery);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, SchemaBuilder.map(Schema.INT32_SCHEMA, Schema.BOOLEAN_SCHEMA))
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValueKafkaConnect);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

    Map<String, Object> bigQueryTestRecord =
        new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testIntegerArray() {
    final String fieldName = "IntegerArray";
    final List<Integer> fieldValue = Arrays.asList(42, 4242, 424242, 42424242);

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldValue);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, SchemaBuilder.array(Schema.INT32_SCHEMA).build())
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValue);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

    Map<String, Object> bigQueryTestRecord =
        new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testStringArray() {
    final String fieldName = "StringArray";
    final List<String> fieldValue =
        Arrays.asList("Forty-two", "forty-two", "Forty two", "forty two");

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldValue);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, SchemaBuilder.array(Schema.STRING_SCHEMA).build())
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValue);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

    Map<String, Object> bigQueryTestRecord =
        new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testBytes() {
    final String fieldName = "Bytes";
    final byte[] fieldBytes = new byte[] {42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54};
    final ByteBuffer fieldValueKafkaConnect = ByteBuffer.wrap(fieldBytes);
    final String fieldValueBigQuery = Base64.getEncoder().encodeToString(fieldBytes);

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldValueBigQuery);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Schema.BYTES_SCHEMA)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValueKafkaConnect);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

    Map<String, Object> bigQueryTestRecord =
        new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testTimestamp() {
    final String fieldName = "Timestamp";
    final long fieldTime = System.currentTimeMillis();
    final java.util.Date fieldValueKafkaConnect = Timestamp.toLogical(
        Timestamp.SCHEMA,
        fieldTime
    );
    final double fieldValueBigQuery = fieldValueKafkaConnect.getTime() / 1000.0;

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldValueBigQuery);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Timestamp.SCHEMA)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValueKafkaConnect);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

    Map<String, Object> bigQueryTestRecord =
        new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testDate() {
    final String fieldName = "Date";
    final int fieldDate = 42;
    final java.util.Date fieldValueKafkaConnect = Date.toLogical(
        Date.SCHEMA,
        fieldDate
    );
    final double fieldValueBigQuery = fieldValueKafkaConnect.getTime() / 1000.0;

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(fieldName, fieldValueBigQuery);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(fieldName, Date.SCHEMA)
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(fieldName, fieldValueKafkaConnect);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

    Map<String, Object> bigQueryTestRecord =
        new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  @Test
  public void testNullable() {
    final String nullableFieldName = "Nullable";
    final String requiredFieldName = "Required";
    final Integer nullableFieldValue = null;
    final Integer requiredFieldValue = 42;

    Map<String, Object> bigQueryExpectedRecord = new HashMap<>();
    bigQueryExpectedRecord.put(requiredFieldName, requiredFieldValue);

    Schema kafkaConnectSchema = SchemaBuilder
        .struct()
        .field(nullableFieldName, SchemaBuilder.int32().optional().build())
        .field(requiredFieldName, SchemaBuilder.int32().required().build())
        .build();

    Struct kafkaConnectStruct = new Struct(kafkaConnectSchema);
    kafkaConnectStruct.put(nullableFieldName, nullableFieldValue);
    kafkaConnectStruct.put(requiredFieldName, requiredFieldValue);
    SinkRecord kafkaConnectRecord = spoofSinkRecord(kafkaConnectSchema, kafkaConnectStruct);

    Map<String, Object> bigQueryTestRecord =
        new BigQueryRecordConverter().convertRecord(kafkaConnectRecord);
    assertEquals(bigQueryExpectedRecord, bigQueryTestRecord);
  }

  private static SinkRecord spoofSinkRecord(Schema valueSchema, Object value) {
    return new SinkRecord(null, 0, null, null, valueSchema, value, 0);
  }
}
