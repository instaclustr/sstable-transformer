#!/bin/bash

VERSION=2.0.0
KEYSPACE="spark_test"
TABLE="test"
# this is for input2
#CREATE_STATEMENT="CREATE TABLE spark_test.test (id bigint PRIMARY KEY, course blob, marks bigint)"
CREATE_STATEMENT="CREATE TABLE spark_test.test3 (id int PRIMARY KEY,col1 int,col2 int)"
#INPUT_DIR="$(pwd)/input-four";  CASSANDRA_VERSION=FOURZERO
# For Cassandra 5 SSTables
INPUT_DIR="$(pwd)/input-five"; CASSANDRA_VERSION=FIVEZERO
#INPUT_DIR="$(pwd)/input2"; CASSANDRA_VERSION=FIVEZERO
OUTPUT_DIR="$(pwd)/output"
COMPRESSION=ZSTD

rm -rf $OUTPUT_DIR
mkdir $OUTPUT_DIR

JVM_OPTIONS="-DSKIP_STARTUP_VALIDATIONS=true -Dfile.encoding=UTF-8 -Djdk.attach.allowAttachSelf=true --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports java.rmi/sun.rmi.server=ALL-UNNAMED --add-exports java.sql/java.sql=ALL-UNNAMED --add-opens java.base/java.lang.module=ALL-UNNAMED --add-opens java.base/jdk.internal.loader=ALL-UNNAMED --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens java.base/jdk.internal.math=ALL-UNNAMED --add-opens java.base/jdk.internal.module=ALL-UNNAMED --add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED"

# REMOTE TRANSFORMATION

#java ${JVM_OPTIONS} -jar target/sstable-transformer-core-$VERSION-bundled.jar transform \
#  --keyspace=${KEYSPACE} \
#  --table=${TABLE} \
#  --compression="${COMPRESSION}" \
#  --output="${OUTPUT_DIR}" \
#  --max-rows-per-file=100000 \
#  --sidecar spark-master-1:9043 --sidecar cassandra-node-1:9043 --sidecar cassandra-node-2:9043 #--partitions=1..20

# LOCAL TRANSFORMATION

# USE --strategy=ONE_FILE_PER_SSTABLE when you do not want to compact

java ${JVM_OPTIONS} -jar target/sstable-transformer-core-$VERSION-bundled.jar transform \
  --cassandra-version="${CASSANDRA_VERSION}" \
  --create-table-statement="${CREATE_STATEMENT}" \
  --compression="${COMPRESSION}" \
  --output="${OUTPUT_DIR}" \
  --input="${INPUT_DIR}" \
  --strategy=ONE_FILE_PER_SSTABLE \
  --sorted \
  --output-format=AVRO

#java ${JVM_OPTIONS} -cp "./target/sstable-transformer-core-2.0.0-bundled.jar:../clickhouse/target/sstable-transformer-clickhouse-2.0.0.jar" \
#    com.instaclustr.transformer.core.Transformer transform \
#      --cassandra-version="FIVEZERO" \
#      --create-table-statement="CREATE TABLE spark_test.testtable (id int primary key)" \
#      --strategy=ONE_FILE_ALL_SSTABLES \
#      --output-format=ARROW_STREAM \
#      --input=/home/fermat/dev/cassandra/sstable-parquet-transformer/clickhouse/src/test/resources/sstables \
#      --sink-config=/home/fermat/dev/cassandra/sstable-parquet-transformer/clickhouse/src/test/resources/clickhouse-sink-byte-buffer.properties