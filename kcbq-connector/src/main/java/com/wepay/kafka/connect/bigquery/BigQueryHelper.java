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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.wepay.kafka.connect.bigquery.utils.GoogleCredentialUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convenience class for creating a default {@link com.google.cloud.bigquery.BigQuery} instance,
 * with or without login credentials.
 */
public class BigQueryHelper {
  private static final Logger logger = LoggerFactory.getLogger(BigQueryHelper.class);

  /**
   * Returns a default {@link BigQuery} instance for the specified project with credentials provided
   * in the specified file, which can then be used for creating, updating, and inserting into tables
   * from specific datasets.
   *
   * @param projectName The name of the BigQuery project to work with
   * @param keyFilename The name of a file containing a JSON key that can be used to provide
   *                    credentials to BigQuery, or null if no authentication should be performed.
   * @return The resulting BigQuery object.
   */
  public BigQuery connect(String projectName, String keyFilename) {
    if (keyFilename == null) {
      return connect(projectName);
    }

    logger.debug("Attempting to authenticate with BigQuery using provided json key");
    return new
        BigQueryOptions.DefaultBigQueryFactory().create(
        BigQueryOptions.newBuilder()
        .setProjectId(projectName)
        .setCredentials(GoogleCredentialUtil.getCredentials(keyFilename))
        .build()
    );
  }

  /**
   * Returns a default {@link BigQuery} instance for the specified project with no authentication
   * credentials, which can then be used for creating, updating, and inserting into tables from
   * specific datasets.
   *
   * @param projectName The name of the BigQuery project to work with
   * @return The resulting BigQuery object.
   */
  public BigQuery connect(String projectName) {
    logger.debug("Attempting to access BigQuery without authentication");
    return new BigQueryOptions.DefaultBigQueryFactory().create(
        BigQueryOptions.newBuilder()
        .setProjectId(projectName)
        .build()
    );
  }
}
