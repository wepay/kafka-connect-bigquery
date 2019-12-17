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

####################################################################################################
# Basic script setup

set -e

if [[ -t 1 ]]; then
	NORMAL="$(tput sgr0)"
	BOLD="$(tput bold)"
	RED="$(tput setaf 1)"
	GREEN="$(tput setaf 2)"
	YELLOW="$(tput setaf 3)"
else
	unset NORMAL BOLD RED GREEN YELLOW
fi

usage() {
  echo -e "usage: $0\n" \
       "[-k|--key-file <JSON key file or JSON key string>]\n" \
       "[-k|--key-source <JSON or FILE>] (path must be absolute; relative paths will not work)\n" \
       "[-p|--project <BigQuery project>]\n" \
       "[-d|--dataset <BigQuery project>]\n" \
       "[-b|--bucket <cloud Storage bucket>\n]" \
       "[-f|--folder <cloud Storage folder under bucket>\n]" \
       1>&2
  echo 1>&2
  echo "Options can also be specified via environment variable:" \
       "KCBQ_TEST_KEYFILE, KCBQ_TEST_PROJECT, KCBQ_TEST_DATASET, KCBQ_TEST_BUCKET, and KCBQ_TEST_FOLDER" \
       "respectively control the keyfile, project, dataset, and bucket." \
       1>&2
  echo 1>&2
  echo "Options can also be specified in a file named 'test.conf'" \
       "placed in the same directory as this script, with a series of <property>=<value> lines." \
       "The properties are 'keyfile', 'project', 'dataset', and 'bucket'." \
       1>&2
  echo 1>&2
  echo "The descending order of priority for each of these forms of specification is:" \
       "command line option, environment variable, configuration file." \
       1>&2
  # Accept an optional exit value parameter
  exit ${1:-0}
}

msg() { printf "$1%s: $2$NORMAL\n" "$(basename $0)"; }
error() { msg "$RED" "$*"; exit 1; } >&2
warn() { msg "$YELLOW" "$*"; } >&2
statusupdate() { msg "$GREEN" "$*"; }
log() { msg "$BOLD" "$*"; }


BASE_DIR=$(dirname "$0")
GRADLEW="$BASE_DIR/../../gradlew"

####################################################################################################
# Configuration processing

# Read in properties file, if it exists and can be read
PROPERTIES_FILE="$BASE_DIR/test.conf"
[[ -f "$PROPERTIES_FILE" ]] && [[ -r "$PROPERTIES_FILE" ]] && source "$PROPERTIES_FILE"

# Copy the file's properties into actual test variables,
# without overriding any that have already been specified
KCBQ_TEST_KEYFILE=${KCBQ_TEST_KEYFILE:-$keyfile}
KCBQ_TEST_PROJECT=${KCBQ_TEST_PROJECT:-$project}
KCBQ_TEST_DATASET=${KCBQ_TEST_DATASET:-$dataset}
KCBQ_TEST_BUCKET=${KCBQ_TEST_BUCKET:-$bucket}
KCBQ_TEST_FOLDER=${KCBQ_TEST_FOLDER:-$folder}
KCBQ_TEST_KEYSOURCE=${KCBQ_TEST_KEYSOURCE:-$keysource}

# Capture any command line flags
while [[ $# -gt 0 ]]; do
  case "$1" in
    -k|--key-file)
        [[ -z "$2" ]] && { error "key filename must follow $1 flag"; usage 1; }
        shift
        KCBQ_TEST_KEYFILE="$1"
        ;;
    -p|--project)
        [[ -z "$2" ]] && { error "project name must follow $1 flag"; usage 1; }
        shift
        KCBQ_TEST_PROJECT="$1"
        ;;
    -d|--dataset)
        [[ -z "$2" ]] && { error "dataset name must follow $1 flag"; usage 1; }
        shift
        KCBQ_TEST_DATASET="$1"
        ;;
    -b|--bucket)
        [[ -z "$2" ]] && { error "bucket name must follow $1 flag"; usage 1; }
        shift
        KCBQ_TEST_BUCKET="$1"
        ;;
    -b|--folder)
        [[ -z "$2" ]] && { error "folder name must follow $1 flag"; usage 1; }
        shift
        KCBQ_TEST_FOLDER="$1"
        ;;
    -h|--help|'-?')
        usage 0
        ;;
    -kf|--key-source)
            [[ -z "$2" ]] && { error "key filename must follow $1 flag"; usage 1; }
            shift
            KCBQ_TEST_KEYSOURCE="$1"
            ;;
    *)
        error "unrecognized option: '$1'"; usage 1
        ;;
  esac
  shift
done

# Make sure required arguments have been provided one way or another
[[ -z "$KCBQ_TEST_KEYFILE" ]] && { error 'a key filename is required'; usage 1; }
[[ -z "$KCBQ_TEST_PROJECT" ]] && { error 'a project name is required'; usage 1; }
[[ -z "$KCBQ_TEST_DATASET" ]] && { error 'a dataset name is required'; usage 1; }
[[ -z "$KCBQ_TEST_BUCKET" ]] && { error 'a bucket name is required'; usage 1; }

####################################################################################################
# Schema Registry Docker initialization

if echo | xargs --no-run-if-empty; then
        xargs() { command xargs --no-run-if-empty "$@"; }
else
        xargs() { command xargs "$@"; }
fi 2> /dev/null


dockercleanup() {
  log 'Cleaning up leftover Docker containers'
  docker ps -aq -f 'name=kcbq_test_(zookeeper|kafka|schema-registry|populate|connect)' \
  | xargs docker rm -f > /dev/null
}

dockerimageexists() {
  docker images --format '{{ .Repository }}' | grep -q "$1"
}

# Cleanup these on exit in case something goes wrong
trap dockercleanup EXIT
# And remove any that are still around right now
dockercleanup

DOCKER_DIR="$BASE_DIR/docker"

ZOOKEEPER_DOCKER_NAME='kcbq_test_zookeeper'
KAFKA_DOCKER_NAME='kcbq_test_kafka'
SCHEMA_REGISTRY_DOCKER_NAME='kcbq_test_schema-registry'

statusupdate 'Creating Zookeeper Docker instance'
docker run --name "$ZOOKEEPER_DOCKER_NAME" \
           -d \
           -e ZOOKEEPER_CLIENT_PORT=32181 \
           confluentinc/cp-zookeeper:4.1.2

statusupdate 'Creating Kafka Docker instance'
docker run --name "$KAFKA_DOCKER_NAME" \
           --link "$ZOOKEEPER_DOCKER_NAME":zookeeper \
           --add-host kafka:127.0.0.1 \
           -d \
           -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:32181 \
           -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092 \
           -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
           confluentinc/cp-kafka:4.1.2

statusupdate 'Creating Schema Registry Docker instance'
# Have to pause here to make sure Zookeeper/Kafka get on their feet first
sleep 5
docker run --name "$SCHEMA_REGISTRY_DOCKER_NAME" \
           --link "$ZOOKEEPER_DOCKER_NAME":zookeeper --link "$KAFKA_DOCKER_NAME":kafka \
           --env SCHEMA_REGISTRY_AVRO_COMPATIBILITY_LEVEL=none \
           -d \
           -e SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL=zookeeper:32181 \
           -e SCHEMA_REGISTRY_HOST_NAME=schema-registry \
           confluentinc/cp-schema-registry:4.1.2

####################################################################################################
# Writing data to Kafka Docker instance via Avro console producer
statusupdate 'Populating Kafka/Schema Registry Docker instances with test data'

POPULATE_DOCKER_IMAGE='kcbq/populate'
POPULATE_DOCKER_NAME='kcbq_test_populate'

if ! dockerimageexists "$POPULATE_DOCKER_IMAGE"; then
  docker build -q -t "$POPULATE_DOCKER_IMAGE" "$DOCKER_DIR/populate"
fi
# Have to pause here to make sure the Schema Registry gets on its feet first
sleep 35
docker create --name "$POPULATE_DOCKER_NAME" \
              --link "$KAFKA_DOCKER_NAME:kafka" --link "$SCHEMA_REGISTRY_DOCKER_NAME:schema-registry" \
              "$POPULATE_DOCKER_IMAGE"
docker cp "$BASE_DIR/resources/test_schemas/" "$POPULATE_DOCKER_NAME:/tmp/schemas/"
docker start -a "$POPULATE_DOCKER_NAME"

####################################################################################################
# Deleting existing BigQuery tables/bucket
warn 'Deleting existing BigQuery test tables and existing GCS bucket'


test_tables=
test_topics=
for file in "$BASE_DIR"/resources/test_schemas/*; do
        test_tables+="${test_tables:+ }kcbq_test_$(basename "${file/-/_}")"
        test_topics+="${test_topics:+,}kcbq_test_$(basename "$file")"
done

"$GRADLEW" -p "$BASE_DIR/.." \
    -Pkcbq_test_keyfile="$KCBQ_TEST_KEYFILE" \
    -Pkcbq_test_project="$KCBQ_TEST_PROJECT" \
    -Pkcbq_test_dataset="$KCBQ_TEST_DATASET" \
    -Pkcbq_test_tables="$test_tables" \
    -Pkcbq_test_bucket="$KCBQ_TEST_BUCKET" \
    -Pkcbq_test_keysource="$KCBQ_TEST_KEYSOURCE" \
    integrationTestPrep

####################################################################################################
# Executing connector in standalone mode (this is the execution portion of the actual test)
statusupdate 'Executing Kafka Connect in Docker'

# Run clean task to ensure there's only one connector tarball in the build/dist directory
"$GRADLEW" -q -p "$BASE_DIR/../.." clean distTar

[[ ! -e "$DOCKER_DIR/connect/properties" ]] && mkdir "$DOCKER_DIR/connect/properties"
RESOURCES_DIR="$BASE_DIR/resources"

STANDALONE_PROPS="$DOCKER_DIR/connect/properties/standalone.properties"
cp "$RESOURCES_DIR/standalone-template.properties" "$STANDALONE_PROPS"

CONNECTOR_PROPS="$DOCKER_DIR/connect/properties/connector.properties"
cp "$RESOURCES_DIR/connector-template.properties" "$CONNECTOR_PROPS"
cat << EOF >> $CONNECTOR_PROPS
project=$KCBQ_TEST_PROJECT
datasets=.*=$KCBQ_TEST_DATASET
gcsBucketName=$KCBQ_TEST_BUCKET
gcsFolderName=$KCBQ_TEST_FOLDER
topics=$test_topics

EOF

CONNECT_DOCKER_IMAGE='kcbq/connect'
CONNECT_DOCKER_NAME='kcbq_test_connect'

cp "$BASE_DIR"/../../kcbq-confluent/build/distributions/kcbq-confluent-*.tar "$DOCKER_DIR/connect/kcbq.tar"
if [[ "$KCBQ_TEST_KEYSOURCE" == "JSON" ]]; then
    echo "$KCBQ_TEST_KEYFILE" > "$DOCKER_DIR/connect/key.json"
else
    cp "$KCBQ_TEST_KEYFILE" "$DOCKER_DIR/connect/key.json"
fi

if ! dockerimageexists "$CONNECT_DOCKER_IMAGE"; then
  docker build -q -t "$CONNECT_DOCKER_IMAGE" "$DOCKER_DIR/connect"
fi
docker create --name "$CONNECT_DOCKER_NAME" \
              --link "$KAFKA_DOCKER_NAME:kafka" --link "$SCHEMA_REGISTRY_DOCKER_NAME:schema-registry" \
              -t "$CONNECT_DOCKER_IMAGE" /bin/bash
docker cp "$DOCKER_DIR/connect/kcbq.tar" "$CONNECT_DOCKER_NAME:/usr/local/share/kafka/plugins/kafka-connect-bigquery/kcbq.tar"
docker cp "$DOCKER_DIR/connect/properties/" "$CONNECT_DOCKER_NAME:/etc/kafka-connect-bigquery/"
docker cp "$DOCKER_DIR/connect/key.json" "$CONNECT_DOCKER_NAME:/tmp/key.json"
docker start -a "$CONNECT_DOCKER_NAME"

####################################################################################################
# Checking on BigQuery data via Java test (this is the verification portion of the actual test)
statusupdate 'Verifying that test data made it successfully to BigQuery'

INTEGRATION_TEST_RESOURCE_DIR="$BASE_DIR/../src/integration-test/resources"
[[ ! -d "$INTEGRATION_TEST_RESOURCE_DIR" ]] && mkdir -p "$INTEGRATION_TEST_RESOURCE_DIR"

cat << EOF > "$INTEGRATION_TEST_RESOURCE_DIR/test.properties"
keyfile=$KCBQ_TEST_KEYFILE
project=$KCBQ_TEST_PROJECT
dataset=$KCBQ_TEST_DATASET
bucket=$KCBQ_TEST_BUCKET
folder=$KCBQ_TEST_FOLDER
keysource=$KCBQ_TEST_KEYSOURCE
EOF


"$GRADLEW" -p "$BASE_DIR/.." cleanIntegrationTest integrationTest
