package com.wepay.kafka.connect.bigquery.api;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Objects;
import java.util.Optional;

public class TopicAndRecordName {
  private final String topic;
  private final String recordName;

  public static TopicAndRecordName from(String topic) {
    return new TopicAndRecordName(topic, null);
  }

  public static TopicAndRecordName from(String topic, String recordName) {
    return new TopicAndRecordName(topic, recordName);
  }

  public static TopicAndRecordName from(SinkRecord record) {
    return from(record.topic(), record.valueSchema().name());
  }

  public TopicAndRecordName(String topic, String recordName) {
    this.topic = topic;
    this.recordName = recordName;
  }

  public String getTopic() {
    return this.topic;
  }

  public Optional<String> getRecordName() {
    return Optional.ofNullable(this.recordName);
  }

  public String toSubject(KafkaSchemaRecordType schemaType) {
    String subject = topic;
    if (recordName == null) {
      subject += "-" + schemaType.toString();
    } else {
      subject += "-" + recordName;
    }
    return subject;
  }

  @Override
  public String toString() {
    return topic + "/" + recordName;
  }

  @Override
  public int hashCode() {
    return this.topic.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof TopicAndRecordName) {
      TopicAndRecordName that = (TopicAndRecordName) obj;
      return Objects.equals(getTopic(), that.getTopic()) && Objects.equals(getRecordName(), that.getRecordName());
    }
    return false;
  }
}
