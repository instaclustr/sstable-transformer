/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.instaclustr.cassandra;

import com.instaclustr.transformer.core.TransformerOptions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.instaclustr.transformer.core.TransformerOptions.Compression.ZSTD;
import static com.instaclustr.transformer.core.TransformerOptions.TransformationStrategy.ONE_FILE_ALL_SSTABLES;
import static com.instaclustr.transformer.core.TransformerOptions.TransformationStrategy.ONE_FILE_PER_SSTABLE;
import static com.instaclustr.transformer.core.TransformerOptions.parseKeyspace;
import static com.instaclustr.transformer.core.TransformerOptions.parsePartitions;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TransformerOptionsTest
{
    @Test
    public void testValidation()
    {
        TransformerOptions options = new TransformerOptions();

        assertThatThrownBy(options::validate)
                .hasMessage("You have not specified --sidecar to read SSTables remotely nor " +
                                    "--input option to read from local disk either. Choose one.");
        options.sidecar = List.of("spark-master-1:9043");
        assertThatThrownBy(options::validate).hasMessage("You have to specify --keyspace for remote data layers");
        options.keyspace = "ks";
        assertThatThrownBy(options::validate).hasMessage("You have to specify --table for remote data layers");
        options.table = "tb";
        assertThatThrownBy(options::validate).hasMessage("--output has to be specified");
        options.output = "/tmp/some-output-dir";
        assertEquals(ONE_FILE_PER_SSTABLE, options.transformationStrategy);
        options.validate();
        assertEquals(ONE_FILE_ALL_SSTABLES, options.transformationStrategy);
        options.validate();

        options = new TransformerOptions();
        options.input = List.of("/tmp/directory-to-read-from");
        assertThatThrownBy(options::validate).hasMessage("You have to specify --create-table-statement for local data layers");
        options.createTableStmt = "CREATE TABLE ks.tb (id int primary key)";
        assertThatThrownBy(options::validate).hasMessage("--output has to be specified");
        options.output = "/tmp/some-output-dir";
        options.validate();

        assertEquals(ONE_FILE_PER_SSTABLE, options.transformationStrategy);
        assertEquals(-1, options.maxRowsPerFile);
        assertFalse(options.bloomFilterEnabled);
        assertEquals(ZSTD, options.compression);

        options.maxRowsPerFile = -100;
        assertThatThrownBy(options::validate).hasMessage("--max-rows-per-file can not be lower than 1");
        options.maxRowsPerFile = 1;
        options.validate();
        options.maxRowsPerFile = 10;
        options.validate();
    }

    @Test
    public void testKeyspaceParsing()
    {
        TransformerOptions options = new TransformerOptions();
        options.createTableStmt = "CREATE TABLE ks.tb (id int primary key)";

        Map<String, String> map = options.forLocalDataLayer();
        assertEquals("ks", map.get("keyspace"));
    }

    @Test
    public void testParseKeyspace()
    {
        // basic
        assertEquals("ks", parseKeyspace("CREATE TABLE ks.tb (id int primary key)"));

        // underscores and digits
        assertEquals("my_keyspace_2", parseKeyspace("CREATE TABLE my_keyspace_2.my_table (id int primary key)"));

        // case insensitive keywords
        assertEquals("ks", parseKeyspace("create table ks.tb (id int primary key)"));
        assertEquals("ks", parseKeyspace("Create Table ks.tb (id int primary key)"));

        // IF NOT EXISTS
        assertEquals("ks", parseKeyspace("CREATE TABLE IF NOT EXISTS ks.tb (id int primary key)"));
        assertEquals("ks", parseKeyspace("create table if not exists ks.tb (id int primary key)"));

        // quoted keyspace
        assertEquals("my-keyspace", parseKeyspace("CREATE TABLE \"my-keyspace\".tb (id int primary key)"));
        assertEquals("My Keyspace", parseKeyspace("CREATE TABLE \"My Keyspace\".tb (id int primary key)"));
        assertEquals("my-ks", parseKeyspace("CREATE TABLE IF NOT EXISTS \"my-ks\".tb (id int primary key)"));

        // no keyspace (should fail)
        assertThatThrownBy(() -> parseKeyspace("CREATE TABLE tb (id int primary key)"))
                .isInstanceOf(IllegalStateException.class);

        // empty string (should fail)
        assertThatThrownBy(() -> parseKeyspace(""))
                .isInstanceOf(IllegalStateException.class);

        // garbage (should fail)
        assertThatThrownBy(() -> parseKeyspace("not a create table statement"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testPartitionParsing()
    {
        TransformerOptions options = new TransformerOptions();

        options.partitions = "1";
        List<Integer> result = parsePartitions(options.partitions);
        assertEquals(1, result.size());
        assertEquals(1, (int) result.get(0));

        options.partitions = "1,2,3,3,4,5";
        result = parsePartitions(options.partitions);

        assertEquals(5, result.size());
        assertEquals(result, List.of(1, 2, 3, 4, 5));

        options.partitions = "1..5";
        result = parsePartitions(options.partitions);
        assertEquals(5, result.size());
        assertEquals(result, List.of(1, 2, 3, 4, 5));

        options.partitions = "";
        assertTrue(parsePartitions(options.partitions).isEmpty());

        assertTrue(parsePartitions(null).isEmpty());

        shouldFail("1...5", "Range partitions have to be in form of 'n..m' where n, m are numbers and n < m.");
        shouldFail("5..1", "Range partitions have to be in form of 'n..m' where n, m are numbers and n < m.");
        shouldFail("a,5", "Range partitions have to be in form of 'n,m,l[...],o'.");
        shouldFail("-1,1", "All partitions have to be positive numbers");
    }

    private void shouldFail(String partitions, String message)
    {
        assertThatThrownBy(() -> parsePartitions(partitions)).hasMessage(message);
    }
}
