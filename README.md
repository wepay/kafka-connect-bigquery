# Kafka Connect BigQuery Connector

[![Source](https://img.shields.io/badge/source-wepay/kafka–connect–bigquery-blue.svg?style=flat-square)](https://github.com/wepay/kafka-connect-bigquery)
[![Release](https://img.shields.io/github/release/wepay/kafka-connect-bigquery.svg?style=flat-square)](https://github.com/wepay/kafka-connect-bigquery/releases)
[![Maven Central](https://img.shields.io/maven-central/v/com.wepay/kafka-connect-bigquery.svg?style=flat-square)](http://search.maven.org/#search%7Cgav%7C1%7Cg%3A%22com.wepay%22%20AND%20a%3A%22kafka-connect-bigquery%22)
[![Open Issues](https://img.shields.io/github/issues/wepay/kafka-connect-bigquery.svg?style=flat-square)](https://github.com/wepay/kafka-connect-bigquery/issues)
[![Pull Requests](https://img.shields.io/github/issues-pr/wepay/kafka-connect-bigquery.svg?style=flat-square)](https://github.com/wepay/kafka-connect-bigquery/pulls)
[![Build Status](https://img.shields.io/travis/wepay/kafka-connect-bigquery.svg?style=flat-square)](https://travis-ci.org/wepay/kafka-connect-bigquery)
[![Code Coverage](https://img.shields.io/codecov/c/github/wepay/kafka-connect-bigquery.svg?style=flat-square)](https://codecov.io/gh/wepay/kafka-connect-bigquery)
[![Code Quality](https://img.shields.io/codacy/grade/547616ddaf5141bc9b10cd48fa9e0acb.svg?style=flat-square)](https://www.codacy.com/app/skyzyx/kafka-connect-bigquery/dashboard)
[![License](https://img.shields.io/github/license/wepay/kafka-connect-bigquery.svg)](https://github.com/wepay/kafka-connect-bigquery/blob/master/LICENSE.md)
[![Author](http://img.shields.io/badge/author-C0urante-blue.svg?style=flat-square)](https://github.com/C0urante)
[![Author](http://img.shields.io/badge/author-mtagle-blue.svg?style=flat-square)](https://github.com/mtagle)

This is an implementation of a sink connector from [Apache Kafka] to [Google BigQuery], built on top 
of [Apache Kafka Connect].

## Standalone Quickstart

> **NOTE**: You must have the [Confluent Platform] installed in order to run the example.

### Configuration Basics

Firstly, you need to specify configuration settings for your connector. These can be found in the 
`quickstart/properties/connector.properties` file. Look for this section:

```plain
########################################### Fill me in! ###########################################
# The name of the BigQuery project to write to
project=
# The name of the BigQuery dataset to write to (leave the '.*=' at the beginning, enter your
# dataset after it)
datasets=.*=
# The location of a BigQuery service account JSON key file
keyfile=
```

You'll need to choose a BigQuery project to write to, a dataset from that project to write to, and
provide the location of a JSON key file that can be used to access a BigQuery service account that
can write to the project/dataset pair. Once you've decided on these properties, fill them in and
save the properties file.

Once you get more familiar with the connector, you might want to revisit the `connector.properties`
file and experiment with tweaking its settings.

### Cherry-Picking Schema Registry Patches

> **NOTE:** You can skip this step if you don't plan on using multiple topics with different names 
but identical schemas, or the [Avro] bytes type.

The connector depends on some commits in Confluent's [Schema Registry] that have not yet been 
included in a released version. As a result, there is some external work involved in building the 
connector. Here is what you need to do before building:

1. Checkout Schema Registry locally.

1. Checkout the `v3.0.0` release tag:

   ```bash
   git checkout v3.0.0
   ```

1. Cherry-pick the following two commits:

   ```bash
   git cherry-pick -m 1 f835af3a2fd97911c633c0a13c72c1d6f91dc1eb
   git cherry-pick -m 1 b3fba7f9f8cc2a117aafa9aff8ac2f50c8dc38e9
   ```

1. Run `mvn install` in your local `schema-registry` directory to install a modified 
   Schema Registry 3.0.0 into your local [Maven] repository.
   
Once a new version of Schema Registry is released, these steps should no longer be required.
   
### Building and Extracting a Tarball

If you haven't already, move into the repository's top-level directory:

```bash
$ cd /path/to/kafka-connect-bigquery/
```

Begin by creating a tarball of the connector with the Confluent Schema Retriever included:

```bash
$ ./gradlew clean confluentTarBall
```

And then extract its contents:

```bash
$ mkdir bin/jar/ && tar -C bin/jar/ -xf bin/tar/kcbq-connector-*-confluent-dist.tar
```

### Setting-Up Background Processes

Then move into the `quickstart` directory:

```bash
$ cd quickstart/
```

After that, if your Confluent Platform installation isn't in a sibling directory to the connector, 
specify its location (and do so before starting each of the subsequent processes in their own 
terminal):

```bash
$ export CONFLUENT_DIR=/path/to/confluent
```

Then, initialize the background processes necessary for Kafka Connect (one terminal per script):
(Taken from http://docs.confluent.io/3.0.0/quickstart.html)

```bash
$ ./zookeeper.sh
```

(wait a little while for it to get on its feet)

```bash
$ ./kafka.sh
```

(wait a little while for it to get on its feet)

```bash
$ ./schema-registry.sh
```

(wait a little while for it to get on its feet)

### Initializing the Avro Console Producer

Next, initialize the Avro Console Producer (also in its own terminal):

```bash
$ ./avro-console-producer.sh
```

Give it some data to start off with (type directly into the Avro Console Producer instance):

```json
{"f1":"Testing the Kafka-BigQuery Connector!"}
```

### Running the Connector

Finally, initialize the BigQuery connector (also in its own terminal):

```bash
$ ./connector.sh
```

### Piping Data Through the Connector

Now you can enter Avro messages of the schema `{"f1": "$SOME_STRING"}` into the Avro Console 
Producer instance, and the pipeline instance should write them to BigQuery.

If you want to get more adventurous, you can experiment with different schemas or topics by 
adjusting flags given to the Avro Console Producer and tweaking the config settings found in the 
`quickstart/properties` directory.

## Integration Testing the Connector

> **NOTE**: You must have [Docker] installed and running on your machine in order to run integration
tests for the connector.

This all takes place in the `kcbq-connector` directory.

### How Integration Testing Works

Integration tests run by creating [Docker] instances for [Zookeeper], [Kafka], [Schema Registry], 
and the BigQuery Connector itself, then verifying the results using a [JUnit] test.

They use schemas and data that can be found in the `test/docker/populate/test_schemas/` directory, 
and rely on a user-provided JSON key file (like in the `quickstart` example) to access BigQuery.

The project and dataset they write to, as well as the specific JSON key file they use, can be
specified by command-line flag, environment variable, or configuration file — the exact details of
each can be found by running the integration test script with the `-?` flag.

### Data Corruption Concerns

In order to ensure the validity of each test, any table that will be written to in the course of
integration testing is preemptively deleted before the connector is run. This will only be an issue
if you have any tables in your dataset whose names begin with `kcbq_test_` and match the sanitized
name of any of the `test_schema` subdirectories. If that is the case, you should probably consider
writing to a different project/dataset.

Because Kafka and Schema Registry are run in Docker, there is no risk that running integration 
tests will corrupt any existing data that is already on your machine, and there is also no need to 
free up any of your ports that might currently be in use by real instances of the programs that are 
faked in the process of testing.

### Running the Integration Tests

Running the series of integration tests is easy:

```bash
$ test/integrationtest.sh
```

This assumes that the project, dataset, and key file have been specified by variable or 
configuration file. For more information on how to specify these, run the test script with
the `--usage` flag.

> **NOTE:** You must have a recent version of [boot2docker], [Docker Machine], [Docker], etc.
installed. Older versions will hang when cleaning containers, and linking doesn't work properly.

### Adding New Integration Tests

Adding an integration test is a little more involved, and consists of two major steps: specifying
Avro data to be sent to Kafka, and specifying via JUnit test how to verify that such data made 
it to BigQuery as expected.

To specify input data, you must create a new directory in the `test/resources/test_schemas/`
directory with whatever name you want the Kafka topic of your test to be named, and whatever 
string you want the name of your test's BigQuery table to be derived from. Then, create two files 
in that directory:

* `schema.json` will contain the Avro schema of the type of data the new test will send
through the connector.

* `data.json` will contain a series of JSON objects, each of which should represent an [Avro] record 
that matches the specified schema. **Each JSON object must occupy its own line, and each object 
cannot occupy more than one line** (this inconvenience is due to limitations in the Avro 
Console Producer, and may be addressed in future commits).

To specify data verification, add a new JUnit test to the file 
`src/integration-test/java/com/wepay/kafka/connect/bigquery/it/BigQueryConnectorIntegrationTest.java`.
Rows that are retrieved from BigQuery in the test are only returned as _Lists_ of _Objects_. The 
names of their columns are not tracked. Construct a _List_ of the _Objects_ that you expect to be 
stored in the test's BigQuery table, retrieve the actual _List_ of _Objects_ stored via a call to 
`readAllRows()`, and then compare the two via a call to `testRows()`.

> **NOTE**: Because the order of rows is not guaranteed when reading test results from BigQuery, 
you must include a row number as the first field of any of your test schemas, and every row of test 
data must have a unique value for its row number (row numbers are one-indexed).

  [Apache Avro]: https://avro.apache.org
  [Apache Kafka Connect]: http://docs.confluent.io/3.0.0/connect/
  [Apache Kafka]: http://kafka.apache.org
  [Apache Maven]: https://maven.apache.org
  [Avro]: https://avro.apache.org
  [BigQuery]: https://cloud.google.com/bigquery/
  [boot2docker]: http://boot2docker.io
  [Confluent Platform]: http://docs.confluent.io/3.0.0/installation.html
  [Docker Machine]: https://docs.docker.com/machine/
  [Docker]: https://www.docker.com
  [Google BigQuery]: https://cloud.google.com/bigquery/
  [JUnit]: http://junit.org
  [Kafka Connect]: http://docs.confluent.io/3.0.0/connect/
  [Kafka]: http://kafka.apache.org
  [Maven]: https://maven.apache.org
  [Schema Registry]: https://github.com/confluentinc/schema-registry
  [Zookeeper]: https://zookeeper.apache.org
