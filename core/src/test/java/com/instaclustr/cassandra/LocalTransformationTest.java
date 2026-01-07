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

import com.instaclustr.transformer.core.SSTableTransformer;
import com.instaclustr.transformer.core.TransformerOptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.instaclustr.cassandra.TransformationVerifier.verify;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.TEST_TABLE_PREFIX;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class LocalTransformationTest extends AbstractTransformationTest
{
    @Test
    public void testTransformation(@TempDir Path outputDir) throws Exception
    {
        TransformerOptions options = getOptions(outputDir);
        // unsorted
        options.sorted = false;
        verify(10_000, new SSTableTransformer(options).runTransformation(), options);

        // sorted
        options.sorted = true;
        verify(10_000, new SSTableTransformer(options).runTransformation(), options);
    }

    @Override
    public TransformerOptions getOptions(Path outputDir)
    {
        TransformerOptions options = new TransformerOptions();
        options.createTableStmt = String.format("CREATE TABLE %s.%s (id int primary key)", TEST_KEYSPACE, TEST_TABLE_PREFIX);
        options.transformationStrategy = TransformerOptions.TransformationStrategy.ONE_FILE_ALL_SSTABLES;
        options.output = outputDir.toAbsolutePath().toString();
        options.input = List.of(getInputDir().toAbsolutePath().toString());

        return options;
    }

    private Path getInputDir()
    {
        Map<String, Object> params = cluster.get(1).config().getParams();
        String[] dataDirs = (String[]) params.get("data_file_directories");
        assert dataDirs.length == 1;

        Path keyspaceDir = Paths.get(dataDirs[0]).resolve(TEST_KEYSPACE);

        try
        {
            List<Path> tables = Files.list(keyspaceDir).collect(Collectors.toList());
            assertEquals(1, tables.size());
            return tables.get(0);
        }
        catch (Throwable t)
        {
            throw new RuntimeException("Unable to list " + keyspaceDir);
        }
    }
}
