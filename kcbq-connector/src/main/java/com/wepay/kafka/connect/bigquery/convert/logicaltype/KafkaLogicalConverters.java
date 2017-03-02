package com.wepay.kafka.connect.bigquery.convert.logicaltype;

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


import com.google.cloud.bigquery.Field;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;

import java.math.BigDecimal;

/**
 * Class containing all the Kafka logical type converters.
 */
public class KafkaLogicalConverters {

  static {
    LogicalConverterRegistry.register(Timestamp.LOGICAL_NAME, new TimestampConverter());
    LogicalConverterRegistry.register(Date.LOGICAL_NAME, new DateConverter());
    LogicalConverterRegistry.register(Decimal.LOGICAL_NAME, new DecimalConverter());

  }

  public static class TimestampConverter extends LogicalTypeConverter {
    public TimestampConverter() {
      super(Timestamp.LOGICAL_NAME,
            Schema.Type.INT64,
            Field.Type.timestamp());
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      return getBqTimestampFormat().format((java.util.Date) kafkaConnectObject);
    }
  }

  public static class DateConverter extends LogicalTypeConverter {
    public DateConverter() {
      super(Date.LOGICAL_NAME,
            Schema.Type.INT32,
            Field.Type.date());
    }

    @Override
    public String convert(Object kafkaConnectObject) {
      return getBQDateFormat().format((java.util.Date) kafkaConnectObject);
    }
  }

  public static class DecimalConverter extends LogicalTypeConverter {
    public DecimalConverter() {
      super(Decimal.LOGICAL_NAME,
            Schema.Type.BYTES,
            Field.Type.floatingPoint());
    }

    @Override
    public BigDecimal convert(Object kafkaConnectObject) {
      // cast to get ClassCastException
      return (BigDecimal) kafkaConnectObject;
    }
  }
}
