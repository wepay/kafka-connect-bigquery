#! /usr/bin/env bash
# Copyright 2016 WePay, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

for schema_dir in /tmp/schemas/*; do

  # Feed single schema topics
  kafka-avro-console-producer \
      --topic "kcbq_test_`basename $schema_dir`" \
      --broker-list 'kafka:29092' \
      --property value.schema="`cat \"$schema_dir/schema.json\"`" \
      --property schema.registry.url='http://schema-registry:8081' \
      < "$schema_dir/data.json"

  # Feed multi schema topic
  MULTI_SCHEMA_TOPIC="kcbq_test_multi"
  kafka-avro-console-producer \
      --topic $MULTI_SCHEMA_TOPIC \
      --broker-list 'kafka:29092' \
      --property schema.registry.url='http://schema-registry:8081' \
      --property value.schema="`cat \"$schema_dir/schema.json\"`" \
      --producer-property value.converter.value.subject.name.strategy=io.confluent.kafka.serializers.subject.TopicRecordNameStrategy \
      --property value.converter.value.subject.name.strategy=io.confluent.kafka.serializers.subject.TopicRecordNameStrategy \
      < "$schema_dir/data.json"

  # Since console producer does not support custom subject naming strategies (like TopicRecordName),
  # we have to instrumentate the schema registry manually.
  # Also we prevent connector from falling back to TopicName strategy silently.
  curl -X DELETE http://schema-registry:8081/subjects/$MULTI_SCHEMA_TOPIC-value &> /dev/null

  # Extract new subject name, according to TopicRecordNameStrategy
  # (https://github.com/confluentinc/schema-registry/blob/4.1.2-post/avro-serializer/src/main/java/io/confluent/kafka/serializers/subject/TopicRecordNameStrategy.java#L39)
  NAME=`jq -r .name $schema_dir/schema.json`
  NAMESPACE=`jq -r .namespace $schema_dir/schema.json`
  SUBJECT=$MULTI_SCHEMA_TOPIC-
  if [ $NAMESPACE == "null" ]; then
    SUBJECT=$SUBJECT$NAME
  else
    SUBJECT=$SUBJECT$NAMESPACE.$NAME
  fi

  curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
      --data "{ \"schema\": \"`cat $schema_dir/schema.json | sed -e 's/"/\\\\"/g' | sed -e ':a' -e 'N' -e '$!ba' -e 's/\n/ /g'`\"}" \
      http://schema-registry:8081/subjects/$SUBJECT/versions &> /dev/null

done
