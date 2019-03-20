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


import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;

import com.wepay.kafka.connect.bigquery.BigQueryHelper;
import com.wepay.kafka.connect.bigquery.GoogleCredentialsReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableClearer {
  private static final Logger logger = LoggerFactory.getLogger(TableClearer.class);

  /**
   * Clears tables in the given project and dataset, using the provided credentials.
   */
  public static void main(String[] args) {
    if (args.length < 5) {
      usage();
    }

    String keyFile = args[0];
    String credentialsStr = args[1];
    String projectName = args[2];
    String dataSet = args[3];

    GoogleCredentials credentials = null;
    if (!credentialsStr.isEmpty()) {
      credentials = GoogleCredentialsReader.fromJsonString(credentialsStr);
    } else if (!keyFile.isEmpty()) {
      credentials = GoogleCredentialsReader.fromJsonFile(keyFile);
    }

    BigQuery bigQuery = new BigQueryHelper().connect(projectName, credentials);
    for (int i = 4; i < args.length; i++) {
      if (bigQuery.delete(dataSet, args[i])) {
        logger.info("Table {} in dataset {} deleted successfully", args[i], dataSet);
      } else {
        logger.info("Table {} in dataset {} does not exist", args[i], dataSet);
      }
    }
  }

  private static void usage() {
    System.err.println(
        "usage: TableClearer <key_file> <credentials> <project_name> <dataset_name> <table> [<table> ...]\n" +
            "<credentials> has priority over <key_file>. " +
                "If <key_file> or <credentials> is empty, it's treated as non-existent."
    );
    System.exit(1);
  }
}
