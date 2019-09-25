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


import com.wepay.kafka.connect.bigquery.config.BigQuerySinkTaskConfig;

import java.util.Map;

public class SinkTaskPropertiesFactory extends SinkPropertiesFactory {
  @Override
  public Map<String, String> getProperties() {
    Map<String, String> properties = super.getProperties();

    properties.put(BigQuerySinkTaskConfig.SCHEMA_UPDATE_CONFIG, "false");

    return properties;
  }

  /**
   * Make sure that each of the default configuration properties work nicely with the given
   * configuration object.
   *
   * @param config The config object to test
   */
  public void testProperties(BigQuerySinkTaskConfig config) {
    super.testProperties(config);

    config.getBoolean(config.SCHEMA_UPDATE_CONFIG);
  }
}
