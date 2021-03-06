package com.wepay.kafka.connect.bigquery.it.utils;

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


import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.BigQuery;

import com.google.cloud.bigquery.Table;
import com.wepay.kafka.connect.bigquery.BigQueryHelper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableClearer {
  private static final Logger logger = LoggerFactory.getLogger(TableClearer.class);
  private static String keySource;


  /**
   * Clears tables in the given project and dataset, using a provided JSON service account key.
   */
  public static void main(String[] args) {
    if (args.length < 5) {
      usage();
    }
    if (args.length == 5) {
      keySource = args[3];
    }
    BigQuery bigQuery = new BigQueryHelper().setKeySource(keySource).connect(args[1], args[0]);
    Page<Table> tables = bigQuery.listTables(args[2]);
    logger.info("Deleting the tables in {} dataset", args[2]);
    for (Table table : tables.iterateAll()) {
      table.delete();
    }
  }

  private static void usage() {
    System.err.println(
        "usage: TableClearer <key_file> <project_name> <dataset_name> <key_source> <table> [<table> ...]"
    );
    System.exit(1);
  }
}
