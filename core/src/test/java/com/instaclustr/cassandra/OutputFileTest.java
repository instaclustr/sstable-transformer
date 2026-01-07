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

import com.google.common.collect.Range;
import com.instaclustr.transformer.core.PartitionAwareFile;
import com.instaclustr.transformer.core.PartitionUnawareFile;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;

import static com.google.common.collect.BoundType.CLOSED;
import static com.google.common.collect.BoundType.OPEN;
import static com.instaclustr.transformer.api.OutputFormat.PARQUET;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class OutputFileTest
{
    @Test
    public void testLocalOutputFile()
    {
        Path path = Paths.get("/tmp/file.parquet");
        PartitionUnawareFile localOutputFile = new PartitionUnawareFile(PARQUET, path.toString());
        assertEquals("/tmp/file.parquet", localOutputFile.getInternalPath().toString());
        assertEquals(1, localOutputFile.getNumber());

        assertEquals("/tmp/file-2.parquet", localOutputFile.next().getPath());
        assertEquals("/tmp/file.parquet", localOutputFile.next().getInternalPath().toString());
        assertEquals(2, localOutputFile.next().getNumber());

        assertEquals("/tmp/file-3.parquet", localOutputFile.next().next().getPath());
        assertEquals("/tmp/file.parquet", localOutputFile.next().next().getInternalPath().toString());
        assertEquals(3, localOutputFile.next().next().getNumber());
    }

    @Test
    public void testPartitionAwareLocalOutputFile()
    {
        Path path = Paths.get("/tmp/file.parquet");
        Range<BigInteger> range = Range.range(new BigInteger("123"), OPEN, new BigInteger("456"), CLOSED);
        PartitionAwareFile localOutputFile = new PartitionAwareFile(PARQUET, path, range, 10, 1);

        assertEquals("/tmp/file.parquet", localOutputFile.getInternalPath().toString());
        assertEquals(10, localOutputFile.getPartition());
        assertEquals(1, localOutputFile.getNumber());
        assertEquals(range, localOutputFile.getTokenRange());
        assertEquals("/tmp/file_123_456-10-1.parquet", localOutputFile.getPath());

        PartitionAwareFile next = localOutputFile.next();
        assertEquals("/tmp/file.parquet", next.getInternalPath().toString());
        assertEquals(10, next.getPartition());
        assertEquals(2, next.getNumber());
        assertEquals(range, next.getTokenRange());
        assertEquals("/tmp/file_123_456-10-2.parquet", next.getPath());

        next = localOutputFile.next().next();
        assertEquals("/tmp/file.parquet", next.getInternalPath().toString());
        assertEquals(10, next.getPartition());
        assertEquals(3, next.getNumber());
        assertEquals(range, next.getTokenRange());
        assertEquals("/tmp/file_123_456-10-3.parquet", next.getPath());
    }
}
