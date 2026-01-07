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

import com.instaclustr.transformer.api.OutputFormat;
import com.instaclustr.transformer.core.AbstractInputOutputFile;
import com.instaclustr.transformer.core.TransformerOptions;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

class TransformationVerifier
{
    public static void verify(int expectedCount,
                              List<Object> outputFiles,
                              TransformerOptions options) throws IOException
    {
        int actualCount = 0;
        for (Object file : outputFiles)
        {
            if (options.outputFormat == OutputFormat.PARQUET)
                actualCount += verifyParquet(file, options);
            else
                actualCount += verifyAvro(file, options);
        }

        assertEquals(expectedCount, actualCount);
    }

    private static int verifyParquet(Object file, TransformerOptions options) throws IOException
    {
        assert file instanceof AbstractInputOutputFile : "file is not an instance of AbstractInputOutputFile";

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder((AbstractInputOutputFile) file)
                .usePageChecksumVerification()
                .useBloomFilter(options.bloomFilterEnabled)
                .build())
        {
            int count = 0;
            GenericRecord record;
            Integer previous = null;
            while ((record = reader.read()) != null)
            {
                Integer next = (Integer) record.get("id");
                if (previous != null && options.sorted)
                {
                    // proof that they are sorted inside file
                    assertTrue(previous < next);
                }
                previous = next;
                ++count;
            }

            if (options.maxRowsPerFile != -1)
                Assert.assertTrue(options.maxRowsPerFile >= count);

            return count;
        }
    }

    private static int verifyAvro(Object file, TransformerOptions options) throws IOException
    {
        assert file instanceof AbstractInputOutputFile : "file is not an instance of AbstractInputOutputFile";

        try (org.apache.avro.file.FileReader<GenericRecord> fileReader = DataFileReader.openReader(new File(((AbstractInputOutputFile) file).getPath()),
                                                                                                   new GenericDatumReader<>()))
        {
            int count = 0;
            GenericRecord record;
            Integer previous = null;
            while (fileReader.hasNext())
            {
                record = fileReader.next();
                Integer next = (Integer) record.get("id");
                if (previous != null && options.sorted)
                {
                    // proof that they are sorted inside file
                    assertTrue(previous < next);
                }
                previous = next;
                ++count;
            }

            if (options.maxRowsPerFile != -1)
                Assert.assertTrue(options.maxRowsPerFile >= count);

            return count;
        }
    }
}
