package com.wepay.kafka.connect.bigquery.write.batch;

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

import com.google.cloud.bigquery.*;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchTableWriterTest {
    private static Storage storage;

    @BeforeClass
    public static void initializeTests() {
        storage = StorageOptions.getDefaultInstance().getService();
    }

    @Test(expected = ValueException.class)
    public void testMapJsonConversion() {
        BatchTableWriter batchTableWriter = new BatchTableWriter("someBucket", "kcbqTest",
                storage,
                TableId.of("",""),
                BigQueryOptions.getDefaultInstance().getService());
        List<Map<String, Object>> records = new ArrayList<>();
        for (int i = 0; i < 3; ++i) {
            Map<String, Object> record = new HashMap<>();
            record.put("f1", i);
            record.put("f2", i+1000);
            records.add(record);
        }

        String expectedString =
                  "{\"f1\":0,\"f2\":1000}\n"
                + "{\"f1\":1,\"f2\":1001}\n"
                + "{\"f1\":2,\"f2\":1002}\n";
        String jsonString = batchTableWriter.toJson(records);
        assertEquals(jsonString, expectedString);
    }

}
