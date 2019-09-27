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

usage() {
	cat <<- EOF
	usage: $(basename $0) <properties>
	 [-k|--key-file <JSON key file>]
	 [-p|--project <BigQuery project>]
	 [-d|--dataset <BigQuery project>]
	 [-b|--bucket <cloud Storage bucket>
	 [-f|--folder <cloud Storage folder under bucket>

	properties must include keyfile, BigQuery dataset name, and a gcloud Storage bucket.
	The keyfile should be a json formatted key for an account that has access to
	the storage bucket and the dataset.  If a BigQuery project is not specified,
	it will be taken from the project_id field of the keyfile.  Properties
	may be specified either as arguments to flags or as key-value pairs.  For example:
	$(basename $0) -k keyfile.json -p project-name -d dataset-name -b bucket-name
	- or -
	$(basename $0) keyfile=key.json project=project-name -d dataset-name -b bucket-name

	Properties can also be specified via environment variable.  eg:
	keyfile=key.json project=project-name $(basename $0) -d dataset-name -b bucket-name

	Properties can also be specified in a file named 'test.conf'
	placed in the same directory as this script, with a series of <property>=<value> lines.
	The properties are 'keyfile', 'project', 'dataset', and 'bucket'.

	The descending order of priority for each of these forms of specification is:
	command line argument, configuration file, environment variable.
	EOF
}

color() {
	test -t 1 || return
	case $1 in
	normal) tput sgr0;;
	bold)   tput bold;;
	red)    tput setaf 1;;
	green)  tput setaf 2;;
	yellow) tput setaf 3;;
	esac
}

msg() {
	color $1
	shift
	printf "%s: %s\n" "$(basename $0)" "$*"
	color normal
}

error() { msg red "$@"; exit 1; } >&2
warn() {  msg yellow "$@"; } >&2
statusupdate() { msg green "$@"; }
log() { msg bold "$@"; }

init() {
	# Initialize global variables, and do some cleanup
	BASE_DIR=$(dirname "$0")
	GRADLEW="$BASE_DIR/../../gradlew"

	# Read in properties file, if it exists and can be read
	. "$BASE_DIR/test.conf" 2> /dev/null

	# Supply -r to xargs if supported.  (This suppresses running the command
	# if there is no input for gnu xargs, avoiding many error messages from
	# dockercleanup, etc.)
	if echo | xargs --no-run-if-empty; then
		xargs() { command xargs --no-run-if-empty "$@"; }
	else
		xargs() { command xargs "$@"; }
	fi 2> /dev/null

	DOCKER_DIR="$BASE_DIR/docker"
	zk_docker_name='kcbq_test_zookeeper'
	kafka_docker_name='kcbq_test_kafka'
	schema_registry_docker_name='kcbq_test_schema-registry'
}

parse_args() {
	folder=kcbq-test  # assign default
	# Process command line
	while test $# -gt 0; do
		case "$1" in
		-k) keyfile="$2"; shift ;;
		-p) project="$2"; shift ;;
		-d) dataset="$2"; shift ;;
		-b) bucket="$2"; shift ;;
		-f) folder="$2"; shift ;;
		-h|--help|'-?') usage ;;
		keyfile=*) keyfile=${1#keyfile=};;
		project=*) project=${1#project=};;
		dataset=*) dataset=${1#dataset=};;
		bucket=*) bucket=${1#bucket=};;
		folder=*) folder=${1#folder-};;
		*) error "unrecognized argument: '$1'" ;;
		esac
		shift
	done

	# Make sure required arguments have been provided one way or another
	[[ -z "$keyfile" ]] && error 'a key filename is required (specify with -k)'
	# If unset, extract project name from the key file
	: ${project:=$( jq -r .project_id $keyfile 2> /dev/null )}
	[[ -z "$project" ]] && error 'a project name is required (specify with -p)'
	[[ -z "$dataset" ]] && error 'a dataset name is required (specify with -d)'
	[[ -z "$bucket" ]] && error 'a bucket name is required (specify with -b)'

	keyfile=$(realpath $keyfile)
}

dockercleanup() {
	log 'Cleaning up Docker containers'
	docker ps -aq -f 'name=kcbq_test_(zookeeper|kafka|schema-registry|populate|connect)' \
	| xargs docker rm -f > /dev/null
}

dockerimageexists() {
	docker images --format '{{ .Repository }}' | grep -q "$1"
}

setup_containers() {
	statusupdate 'Creating Zookeeper Docker instance'
	docker run \
		--name "$zk_docker_name" \
		-d \
		-e ZOOKEEPER_CLIENT_PORT=32181 \
		confluentinc/cp-zookeeper:4.1.2

	statusupdate 'Creating Kafka Docker instance'
	docker run \
		--name "$kafka_docker_name" \
		--link "$zk_docker_name":zookeeper \
		--add-host kafka:127.0.0.1 \
		-d \
		-e KAFKA_ZOOKEEPER_CONNECT=zookeeper:32181 \
		-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:29092 \
		-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
		confluentinc/cp-kafka:4.1.2

	statusupdate 'Creating Schema Registry Docker instance'
	# Have to pause here to make sure Zookeeper/Kafka get on their feet first
	sleep 5
	docker run \
		--name "$schema_registry_docker_name" \
		--link "$zk_docker_name":zookeeper --link "$kafka_docker_name":kafka \
		--env SCHEMA_REGISTRY_AVRO_COMPATIBILITY_LEVEL=none \
		-d \
		-e SCHEMA_REGISTRY_KAFKASTORE_CONNECTION_URL=zookeeper:32181 \
		-e SCHEMA_REGISTRY_HOST_NAME=schema-registry \
		confluentinc/cp-schema-registry:4.1.2

	statusupdate 'Populating Kafka/Schema Registry Docker instances with test data'

	populate_docker_image='kcbq/populate'
	populate_docker_name='kcbq_test_populate'

	if ! dockerimageexists "$populate_docker_image"; then
		docker build -q -t "$populate_docker_image" "$DOCKER_DIR/populate"
	fi
	# Have to pause here to make sure the Schema Registry gets on its feet first
	sleep 35
	docker create \
		--name "$populate_docker_name" \
		--link "$kafka_docker_name:kafka" \
		--link "$schema_registry_docker_name:schema-registry" \
		"$populate_docker_image"
	docker cp "$BASE_DIR/resources/test_schemas/" "$populate_docker_name:/tmp/schemas/"
	docker start -a "$populate_docker_name"
}

cleanup_bq() {
	####################################################################################################
	# Deleting existing BigQuery tables/bucket
	warn 'Deleting existing BigQuery test tables and existing GCS bucket'

	"$GRADLEW" -p "$BASE_DIR/.." \
		-Pkcbq_test_keyfile="$keyfile" \
		-Pkcbq_test_project="$project" \
		-Pkcbq_test_dataset="$dataset" \
		-Pkcbq_test_tables="test_tables" \
		-Pkcbq_test_bucket="$bucket" \
		integrationTestPrep
}

build_clean_dist() {
	"$GRADLEW" -q -p "$BASE_DIR/../.." clean distTar
}

run_kafka_connect() {
	test_topics=
	for file in "$BASE_DIR"/resources/test_schemas/*; do
		test_topics+="${test_topics:+,}$(basename "kcbq_test_$file")"
	done

	statusupdate 'Executing Kafka Connect in Docker'
	mkdir -p "$DOCKER_DIR/connect/properties"

	STANDALONE_PROPS="$DOCKER_DIR/connect/properties/standalone.properties"
	cp "$BASE_DIR/resources/standalone-template.properties" "$STANDALONE_PROPS"

	{
	cat "$BASE_DIR/resources/connector-template.properties"
	cat <<- EOF
	project=$project
	datasets=.*=$dataset
	gcsBucketName=$bucket
	gcsFolderName=$folder
	topics=$test_topics

	EOF
	} > "$DOCKER_DIR/connect/properties/connector.properties"

	CONNECT_DOCKER_IMAGE='kcbq/connect'
	CONNECT_DOCKER_NAME='kcbq_test_connect'
	cp "$BASE_DIR"/../../kcbq-confluent/build/distributions/kcbq-confluent-*.tar "$DOCKER_DIR/connect/kcbq.tar"
	cp "$keyfile" "$DOCKER_DIR/connect/key.json"

	if ! dockerimageexists "$CONNECT_DOCKER_IMAGE"; then
	  docker build -q -t "$CONNECT_DOCKER_IMAGE" "$DOCKER_DIR/connect"
	fi
	docker create --name "$CONNECT_DOCKER_NAME" \
		      --link "$kafka_docker_name:kafka" --link "$schema_registry_docker_name:schema-registry" \
		      -t "$CONNECT_DOCKER_IMAGE" /bin/bash
	docker cp "$DOCKER_DIR/connect/kcbq.tar" "$CONNECT_DOCKER_NAME:/usr/local/share/kafka/plugins/kafka-connect-bigquery/kcbq.tar"
	docker cp "$DOCKER_DIR/connect/properties/" "$CONNECT_DOCKER_NAME:/etc/kafka-connect-bigquery/"
	docker cp "$DOCKER_DIR/connect/key.json" "$CONNECT_DOCKER_NAME:/tmp/key.json"
	docker start -a "$CONNECT_DOCKER_NAME"
}


run_integration_test() {
	# Checking on BigQuery data via Java test (this is the verification portion of the actual test)
	statusupdate 'Verifying that test data made it successfully to BigQuery'

	dir="$BASE_DIR/../src/integration-test/resources"
	mkdir -p "$dir"
	{
		echo "keyfile=$keyfile"
		echo "project=$project"
		echo "dataset=$dataset"
		echo "bucket=$bucket"
		echo "folder=$folder"
	} > "$dir/test.properties"

	"$GRADLEW" -p "$BASE_DIR/.." cleanIntegrationTest integrationTest
}

main() {
	init
	parse_args "$@"
	trap dockercleanup EXIT
	dockercleanup

	setup_containers
	cleanup_bq
	build_clean_dist
	run_kafka_connect
	run_integration_test
}

main "$@"
