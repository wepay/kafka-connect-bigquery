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
import com.wepay.kafka.connect.bigquery.exception.ConversionConnectException;
import org.apache.kafka.connect.data.Schema;

import java.text.SimpleDateFormat;

/**
 * Abstract class for logical type converters.
 * Contains logic for both schema and record conversions.
 */
public abstract class LogicalTypeConverter {

  private String logicalName;
  private Schema.Type encodingType;
  private Field.Type bqSchemaType;

  /**
   * Create a new LogicalConverter.
   *
   * @param logicalName The name of the logical type.
   * @param encodingType The encoding type of the logical type.
   * @param bqSchemaType The corresponding BigQuery Schema type of the logical type.
   */
  public LogicalTypeConverter(String logicalName,
                              Schema.Type encodingType,
                              Field.Type bqSchemaType) {
    this.logicalName = logicalName;
    this.encodingType = encodingType;
    this.bqSchemaType = bqSchemaType;
  }

  /**
   * Checks if the given schema encoding type is the same as the expected encoding type.
   * Throws an {@link ConversionConnectException} if not.
   *
   * @param encodingType the encoding type to check.
   * @throws ConversionConnectException
   */
  public void checkEncodingType(Schema.Type encodingType) throws ConversionConnectException {
    if (encodingType != this.encodingType) {
      throw new ConversionConnectException(
        "Logical Type " + logicalName + " must be encoded as " + this.encodingType + "; "
          + "instead, found " + encodingType
      );
    }
  }

  public Field.Type getBQSchemaType() {
    return bqSchemaType;
  }

  /**
   * Convert the given KafkaConnect Record Object to a BigQuery Record Object.
   *
   * @param kafkaConnectObject the kafkaConnectObject
   * @return the converted Object
   */
  public abstract Object convert(Object kafkaConnectObject);

  protected static SimpleDateFormat getBqTimestampFormat() {
    return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  }

  protected static SimpleDateFormat getBQDatetimeFormat() {
    return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
  }

  protected static SimpleDateFormat getBQDateFormat() {
    return new SimpleDateFormat("yyyy-MM-dd");
  }

  protected static SimpleDateFormat getBQTimeFormat() {
    return new SimpleDateFormat("HH:mm:ss.SSS");
  }

}
