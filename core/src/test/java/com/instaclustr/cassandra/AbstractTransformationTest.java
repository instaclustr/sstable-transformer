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
import com.instaclustr.transformer.core.TransformerOptions.Compression;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static com.instaclustr.cassandra.TransformationVerifier.verify;
import static com.instaclustr.transformer.api.OutputFormat.AVRO;
import static com.instaclustr.transformer.api.OutputFormat.PARQUET;

public abstract class AbstractTransformationTest extends AbstractIntegrationTest
{
    @Test
    public void testParquetCompressors(@TempDir Path outputDir) throws Exception
    {
        TransformerOptions options = getOptions(outputDir);
        options.outputFormat = PARQUET;

        for (Compression compression : Compression.values())
        {
            options.compression = compression;
            verify(10_000, new SSTableTransformer(options).runTransformation(), options);
        }
    }

    @Test
    public void testAvroCompressors(@TempDir Path outputDir) throws Exception
    {
        TransformerOptions options = getOptions(outputDir);
        options.outputFormat = AVRO;

        for (Compression compression : Compression.values())
        {
            options.compression = compression;
            verify(10_000, new SSTableTransformer(options).runTransformation(), options);
        }
    }

    @Test
    public void testMaxNumberOfRows(@TempDir Path outputDir) throws Exception
    {
        TransformerOptions options = getOptions(outputDir);
        options.maxRowsPerFile = 100;

        options.sorted = false;
        verify(10_000, new SSTableTransformer(options).runTransformation(), options);

        options.sorted = true;
        verify(10_000, new SSTableTransformer(options).runTransformation(), options);
    }

    @Override
    protected void afterClusterProvisioned()
    {
        cluster.get(1).nodetoolResult("disableautocompaction").asserts().success();
    }

    public abstract TransformerOptions getOptions(Path outPutDir);
}
