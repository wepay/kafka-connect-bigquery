package com.wepay.kafka.connect.bigquery.partition;

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


import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.exception.BigQueryConnectException;
import org.apache.kafka.connect.data.Schema;

import java.util.List;
import java.util.Set;

/**
 * Interface for partitioning lists of elements and writing those partitions to bigQuery.
 * @param <E> The type of element in the list that will be partitioned.
 */
public interface Partitioner<E> {
  /**
   * @param elements The list of elements to write to BigQuery.
   */
  void writeAll(TableId table, List<E> elements, String topic, Set<Schema> schemas)
      throws BigQueryConnectException, InterruptedException;
}
