package com.wepay.kafka.connect.bigquery;

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


import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import com.wepay.kafka.connect.bigquery.convert.SchemaConverter;

import org.apache.kafka.connect.data.Schema;

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

public class SchemaManagerTest {

  @Test
  public void testBQTableDescription() {
    final String testTableName = "testTable";
    final String testDatasetName = "testDataset";
    final String testDoc = "test doc";
    final TableId tableId = TableId.of(testDatasetName, testTableName);

    SchemaRetriever mockSchemaRetriever = mock(SchemaRetriever.class);
    @SuppressWarnings("unchecked")
    SchemaConverter<com.google.cloud.bigquery.Schema> mockSchemaConverter =
        (SchemaConverter<com.google.cloud.bigquery.Schema>) mock(SchemaConverter.class);
    BigQuery mockBigQuery = mock(BigQuery.class);

    Optional<String> kafkaKeyFieldName = Optional.of("kafkaKey");
    Optional<String> kafkaDataFieldName = Optional.of("kafkaData");

    SchemaManager schemaManager = new SchemaManager(mockSchemaRetriever,
                                                    mockSchemaConverter,
                                                    mockBigQuery,
                                                    kafkaKeyFieldName,
                                                    kafkaDataFieldName);

    Schema mockKafkaSchema = mock(Schema.class);
    // we would prefer to mock this class, but it is final.
    com.google.cloud.bigquery.Schema fakeBigQuerySchema =
        com.google.cloud.bigquery.Schema.of(Field.of("mock field", LegacySQLTypeName.STRING));

    when(mockSchemaConverter.convertSchema(mockKafkaSchema)).thenReturn(fakeBigQuerySchema);
    when(mockKafkaSchema.doc()).thenReturn(testDoc);

    TableInfo tableInfo = schemaManager.constructTableInfo(tableId, mockKafkaSchema, mockKafkaSchema);

    Assert.assertEquals("Kafka doc does not match BigQuery table description",
                        testDoc, tableInfo.getDescription());
  }
}
