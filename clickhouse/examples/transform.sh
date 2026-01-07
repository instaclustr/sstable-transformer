#!/bin/bash

JVM_OPTIONS="-DSKIP_STARTUP_VALIDATIONS=true -Dfile.encoding=UTF-8 -Djdk.attach.allowAttachSelf=true --add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-exports java.base/sun.nio.ch=ALL-UNNAMED --add-exports java.management.rmi/com.sun.jmx.remote.internal.rmi=ALL-UNNAMED --add-exports java.rmi/sun.rmi.registry=ALL-UNNAMED --add-exports java.rmi/sun.rmi.server=ALL-UNNAMED --add-exports java.sql/java.sql=ALL-UNNAMED --add-opens java.base/java.lang.module=ALL-UNNAMED --add-opens java.base/jdk.internal.loader=ALL-UNNAMED --add-opens java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/jdk.internal.reflect=ALL-UNNAMED --add-opens java.base/jdk.internal.math=ALL-UNNAMED --add-opens java.base/jdk.internal.module=ALL-UNNAMED --add-opens java.base/jdk.internal.util.jar=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED"

# Examples of tool invocation, change the parameters as you see fit to accommodate your case.

# Only transforms to Parquet, does not load to Clickhouse.
# This example takes SSTables (e.g in a (snapshot) directory (preferable)
# Then it will transform SSTable to Parquet file in "--output" directory.
# Rows in Parquet files will not be --sorted.
# There will be one thread processing all SSTables by --parallelism=1
# There will be at maximum 1 million rows in any Parquet file. If it overflows, then new Parquet file will be created.
# Data will be transformed to --output directory.
# We do not need any sink, so we do not place any sink implementation on class path (-cp)
toParquet() {
  java ${JVM_OPTIONS} \
    -Xmx16384M \
    -XX:+UnlockExperimentalVMOptions \
    -XX:+UseZGC \
    -cp "./sstable-transformer-core-2.0.0-bundled.jar" \
    com.instaclustr.transformer.core.Transformer transform \
    --cassandra-version="FIVEZERO" \
    --create-table-statement="CREATE TABLE cassandra_easy_stress.shuffled_text (key text PRIMARY KEY, value text)" \
    --strategy=ONE_FILE_PER_SSTABLE \
    --output-format=PARQUET \
    --parallelism=1 \
    --max-rows-per-file=1000000 \
    --input=/var/lib/cassandra/data/cassandra_easy_stress/shuffled_text-21c0345021f311f19d05bba1bd4c3ce1 \
    --output=/submit/parquet-data
}

# Transforms SSTable to Parquet and that file is inserted into Clickhouse via HTTP.
# We need ClickHouse sink which will take transformed SSTable into Parquet and then this file is
# in turn inserted via HTTP to ClickHouse.
# There will 1 thread transforming SSTables to Parquet files, each file will have at most 500k rows.
# We need to also specify --sink-config for the configuration of the sink.
toClickhouseViaParquetSink() {
  java ${JVM_OPTIONS} \
    -Xmx16384M \
    -XX:+UnlockExperimentalVMOptions \
    -XX:+UseZGC \
    -cp "./sstable-transformer-core-2.0.0-bundled.jar:./sstable-transformer-clickhouse-2.0.0.jar" \
    com.instaclustr.transformer.core.Transformer transform \
    --cassandra-version="FIVEZERO" \
    --create-table-statement="CREATE TABLE cassandra_easy_stress.shuffled_text (key text PRIMARY KEY, value text)" \
    --strategy=ONE_FILE_PER_SSTABLE \
    --output-format=PARQUET \
    --output=/submit/parquet-data \
    --parallelism=1 \
    --max-rows-per-file=500000 \
    --input=/var/lib/cassandra/data/cassandra_easy_stress/shuffled_text-21c0345021f311f19d05bba1bd4c3ce1 \
    --sink-config=/submit/transform/clickhouse-sink-file.properties
}

# Reads one continuous stream of data from SSTable and populates a buffer.
# This transformation is not creating intermediate Parquet files, --output-format is ARROW_STREAM,
# meaning that read data from SSTables are converted into Arrow Stream and buffer of these data are
# inserted in ClickHouse. The advantage of this approach is that we never go to disk to write Parquet files to insert,
# we load it directly from memory.
toClickhouseViaBufferSink() {
  java ${JVM_OPTIONS} \
    -Xmx16384M \
    -XX:+UnlockExperimentalVMOptions \
    -XX:+UseZGC \
    -cp "./sstable-transformer-core-2.0.0-bundled.jar:./sstable-transformer-clickhouse-2.0.0.jar" \
    com.instaclustr.transformer.core.Transformer transform \
    --cassandra-version="FIVEZERO" \
    --create-table-statement="CREATE TABLE cassandra_easy_stress.shuffled_text (key text PRIMARY KEY, value text)" \
    --strategy=ONE_FILE_PER_SSTABLE \
    --output-format=ARROW_STREAM \
    --parallelism=1 \
    --input=/var/lib/cassandra/data/cassandra_easy_stress/shuffled_text-21c0345021f311f19d05bba1bd4c3ce1 \
    --sink-config=/submit/transform/clickhouse-sink-pipe.properties
}

#toParquet
#toClickhouseViaParquetSink
toClickhouseViaBufferSink
