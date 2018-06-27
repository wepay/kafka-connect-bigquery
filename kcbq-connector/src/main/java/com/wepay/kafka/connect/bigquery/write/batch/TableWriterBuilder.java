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


import org.apache.kafka.connect.sink.SinkRecord;

/**
 * Interface for building a {@link TableWriter} or TableWriterGCS
 */
public interface TableWriterBuilder<R> {

    /**
     * Add a record to the builder.
     * @param recordToInsert the record to add.
     */
    void addRecord(SinkRecord recordToInsert);

    /**
     * Create a {@link TableWriter} from this builder.
     * @return a TableWriter containing the given writer, table, topic, and all added rows.
     */
    R build();
}
