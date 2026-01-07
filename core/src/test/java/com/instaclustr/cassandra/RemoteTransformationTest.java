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

import com.instaclustr.transformer.core.DataLayerHelpers;
import com.instaclustr.transformer.core.DataLayerWrapper;
import com.instaclustr.transformer.core.SSTableTransformer;
import com.instaclustr.transformer.core.TransformerOptions;
import org.apache.cassandra.spark.data.DataLayer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.instaclustr.cassandra.TransformationVerifier.verify;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.TEST_TABLE_PREFIX;

public class RemoteTransformationTest extends AbstractTransformationTest
{
    @Test
    public void testTransformation(@TempDir Path outputDir) throws Exception
    {
        TransformerOptions tmpOpts = getOptions(outputDir);
        DataLayerWrapper wrapper = new ArrayList<>(DataLayerHelpers.getDataLayerWrappers(tmpOpts)).get(0);
        DataLayer dataLayer = wrapper.getDataLayer();
        int i = dataLayer.partitionCount();
        String partitions = "0.." + (i - 1);

        TransformerOptions options = getOptions(outputDir);
        options.partitions = partitions;
        // unsorted
        options.sorted = false;
        verify(10_000, new SSTableTransformer(options).runTransformation(), tmpOpts);

        // sorted
        options.sorted = true;
        verify(10_000, new SSTableTransformer(options).runTransformation(), tmpOpts);
    }


    @Override
    public TransformerOptions getOptions(Path outPutDir)
    {
        TransformerOptions options = new TransformerOptions();
        options.sidecar = List.of("localhost:" + server.actualPort());
        options.keyspace = TEST_KEYSPACE;
        options.table = TEST_TABLE_PREFIX;
        options.transformationStrategy = TransformerOptions.TransformationStrategy.ONE_FILE_ALL_SSTABLES;
        options.output = outPutDir.toAbsolutePath().toString();
        options.keepSnapshot = true;

        return options;
    }
}
