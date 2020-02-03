package com.wepay.kafka.connect.bigquery.api;

import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Objects;
import java.util.Optional;

/**
 * A container class with separate topic and (optional) record names.
 */
public class TopicAndRecordName {
  private final String topic;
  private final String recordName;

  /**
   * Convenience method for creating a new {@link TopicAndRecordName} with empty record name.
   *
   * @param topic the topic.
   * @return a new {@link TopicAndRecordName} with empty record name.
   */
  public static TopicAndRecordName from(String topic) {
    return new TopicAndRecordName(topic, null);
  }

  /**
   * Convenience method for creating a new {@link TopicAndRecordName}.
   *
   * @param topic the topic.
   * @param recordName the record name.
   * @return a new {@link TopicAndRecordName}.
   */
  public static TopicAndRecordName from(String topic, String recordName) {
    return new TopicAndRecordName(topic, recordName);
  }

  /**
   * Convenience method for creating a new {@link TopicAndRecordName} from {@link SinkRecord}.
   *
   * @param record the record used to extract topic and (optionally) record name.
   * @param includeRecordName whether or not to include the record name.
   * @return a new {@link TopicAndRecordName}.
   */
  public static TopicAndRecordName from(SinkRecord record, boolean includeRecordName) {
    String maybeRecordName = includeRecordName ? record.valueSchema().name() : null;
    return from(record.topic(), maybeRecordName);
  }

  /**
   * Create a new {@link TopicAndRecordName}.
   *
   * @param topic the topic
   * @param recordName the record name
   */
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

  @Override
  public String toString() {
    return topic + "/" + recordName;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.topic) + Objects.hashCode(this.recordName);
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
