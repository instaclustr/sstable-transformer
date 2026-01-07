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
package com.instaclustr.transformer.core;

import com.google.common.collect.Range;
import com.instaclustr.transformer.api.OutputFormat;

import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Output file which takes partition and ranges into consideration when it comes to its name on a disk.
 * <p>
 * The name of a file like this will be in format of: {@code filename-rangeOpen_rangeClose-partition-number.parquet}
 */
public class PartitionAwareFile extends AbstractInputOutputFile
{
    private final Path path;
    private final int partition;
    private final Range<BigInteger> tokenRange;

    /**
     * @param outputFormat format to use for writing
     * @param path         path to a file
     * @param tokenRange   token range, in Cassandra's terms, this file will be holding data of
     * @param partition    Spark partition this file is of
     * @param number       ever-increasing number appended to the end of a filename in case previous file is full.
     */
    public PartitionAwareFile(OutputFormat outputFormat,
                              Path path,
                              Range<BigInteger> tokenRange,
                              int partition,
                              int number)
    {
        super(outputFormat, path, number);
        this.partition = partition;
        this.tokenRange = tokenRange;
        this.path = resolvePath();
    }

    @Override
    public PartitionAwareFile next()
    {
        return new PartitionAwareFile(getOutputFormat(),
                                      getInternalPath(),
                                      tokenRange,
                                      partition,
                                      nextNumber());
    }

    @Override
    public String getPath()
    {
        return path.toAbsolutePath().toString();
    }

    public int getPartition()
    {
        return partition;
    }

    public Range<BigInteger> getTokenRange()
    {
        return tokenRange;
    }

    private Path resolvePath()
    {
        String fileExtension = getOutputFormat().getFileExtension();
        String suffix = String.format("_%s_%s-%s-%s%s",
                                      tokenRange.lowerEndpoint(),
                                      tokenRange.upperEndpoint(),
                                      partition,
                                      getNumber(),
                                      fileExtension);
        String toReplaceIn = getInternalPath().toAbsolutePath().toString();
        int lastIndex = toReplaceIn.lastIndexOf(fileExtension);
        String replaced = toReplaceIn.substring(0, lastIndex) + suffix;
        return Paths.get(replaced);
    }
}
