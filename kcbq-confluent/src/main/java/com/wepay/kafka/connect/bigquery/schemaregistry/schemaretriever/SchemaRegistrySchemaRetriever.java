package com.wepay.kafka.connect.bigquery.schemaregistry.schemaretriever;

import com.google.cloud.bigquery.TableId;

import com.wepay.kafka.connect.bigquery.api.SchemaRetriever;

import com.wepay.kafka.connect.bigquery.api.TopicAndRecordName;
import io.confluent.connect.avro.AvroData;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import org.apache.avro.Schema.Parser;

import org.apache.kafka.connect.data.Schema;

import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Uses the Confluent Schema Registry to fetch the latest schema for a given topic.
 */
public class SchemaRegistrySchemaRetriever implements SchemaRetriever {
  private static final Logger logger = LoggerFactory.getLogger(SchemaRegistrySchemaRetriever.class);

  private SchemaRegistryClient schemaRegistryClient;
  private AvroData avroData;

  /**
   * Only here because the package-private constructor (which is only used in testing) would
   * otherwise cover up the no-args constructor.
   */
  public SchemaRegistrySchemaRetriever() {
  }

  // For testing purposes only
  SchemaRegistrySchemaRetriever(SchemaRegistryClient schemaRegistryClient, AvroData avroData) {
    this.schemaRegistryClient = schemaRegistryClient;
    this.avroData = avroData;
  }

  @Override
  public void configure(Map<String, String> properties) {
    SchemaRegistrySchemaRetrieverConfig config =
        new SchemaRegistrySchemaRetrieverConfig(properties);
    Map<String, ?> schemaRegistryClientProperties =
        config.originalsWithPrefix(SchemaRegistrySchemaRetrieverConfig.SCHEMA_REGISTRY_CLIENT_PREFIX);
    schemaRegistryClient = new CachedSchemaRegistryClient(
        config.getString(SchemaRegistrySchemaRetrieverConfig.LOCATION_CONFIG),
        0,
        schemaRegistryClientProperties
    );
    avroData = new AvroData(config.getInt(SchemaRegistrySchemaRetrieverConfig.AVRO_DATA_CACHE_SIZE_CONFIG));
  }

  @Override
  public Schema retrieveSchema(TableId table, TopicAndRecordName topicAndRecordName) throws ConnectException {
    return retrieveSchema(topicAndRecordName);
  }

  @Override
  public Map<TopicAndRecordName, Schema> retrieveSchemas(List<String> topics, Map<Pattern, String> recordAliases) throws ConnectException {
    Map<String, TopicAndRecordName> matchingSubjects = getSubjects(topics, recordAliases);
    return matchingSubjects.values().stream()
        .map(topicAndRecordName -> new AbstractMap.SimpleEntry<>(topicAndRecordName, retrieveSchema(topicAndRecordName)))
        .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
  }

  private boolean subjectMatchesRegex(String subject, Pattern regex, String topic) {
    if (!subject.startsWith(topic)) {
      return false;
    }
    String recordName = String.join(topic, Arrays.stream(subject.split(topic + "-")).filter(s -> !s.trim().isEmpty()).collect(Collectors.toSet()));
    return regex.matcher(recordName).matches();
  }

  private Schema retrieveSchema(TopicAndRecordName topicAndRecordName) throws ConnectException {
    String topic = topicAndRecordName.getTopic();
    String subject = topicAndRecordName.toSubject();
    logger.debug("Retrieving schema information for topic {} with subject {}", topic, subject);
    try {
      SchemaMetadata latestSchemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(subject);
      org.apache.avro.Schema avroSchema = new Parser().parse(latestSchemaMetadata.getSchema());
      return avroData.toConnectSchema(avroSchema);
    } catch (IOException | RestClientException exception) {
      throw new ConnectException(String.format(
          "Exception while fetching latest schema metadata for topic=%s, subject=%s",
          topic, subject),
          exception
      );
    }
  }

  @Override
  public void setLastSeenSchema(TableId table, TopicAndRecordName topicAndRecordName, Schema schema) {
  }

  private Map<String, TopicAndRecordName> getSubjects(List<String> topics, Map<Pattern, String> recordAliases) {
    Collection<String> subjects;
    try {
      subjects = schemaRegistryClient.getAllSubjects();
    } catch (IOException | RestClientException exception) {
      throw new ConnectException("Exception while fetching subjects", exception);
    }
    Set<Pattern> recordNamePatterns = recordAliases.keySet();
    return topics.stream().flatMap(topic ->
        subjects.stream().filter(subject ->
            recordNamePatterns.stream()
                .anyMatch(pattern -> subjectMatchesRegex(subject, pattern, topic))
        ).map(matchingSubject -> new AbstractMap.SimpleEntry<>(matchingSubject, extractTopicAndRecord(topic, matchingSubject)))
    ).collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
  }

  private TopicAndRecordName extractTopicAndRecord(String topic, String subject) {
    String recordName = subject;
    if (subject.startsWith(topic)) {
      recordName = subject.split(Pattern.quote(topic + "-"), 2)[1];
    }
    if (recordName.equals("value")) {
      return TopicAndRecordName.from(topic);
    }
    return TopicAndRecordName.from(topic, recordName);

  }
}
