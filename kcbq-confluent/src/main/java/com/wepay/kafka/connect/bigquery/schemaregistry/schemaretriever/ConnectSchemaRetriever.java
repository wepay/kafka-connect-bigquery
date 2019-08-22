package com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever;

import com.google.cloud.bigquery.TableId;
import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class ConnectSchemaRetriever implements SchemaRetriever {
  private static final Logger logger = LoggerFactory.getLogger(SchemaRegistrySchemaRetriever.class);

  /**
   * Only here because the package-private constructor (which is only used in testing) would
   * otherwise cover up the no-args constructor.
   */
  public ConnectSchemaRetriever() {
  }

  @Override
  public void configure(Map<String, String> properties) {
  }

  @Override
  public Schema retrieveSchema(TableId table, String topic) {
    return null;
  }

  @Override
  public void setLastSeenSchema(TableId table, String topic, Schema schema) {

  }

  public Schema retrieveSchema(SinkRecord record) {
    if (record.valueSchema() == null) {
      throw new ConnectException("Fail to retrieve connect schema from record.");
    }
    return record.valueSchema();
  }
}
