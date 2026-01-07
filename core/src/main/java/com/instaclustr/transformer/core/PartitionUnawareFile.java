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

import com.instaclustr.transformer.api.OutputFormat;

import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Output file for {@link org.apache.cassandra.spark.data.LocalDataLayer} which does not
 * have any concept of partitions or ranges.
 */
public class PartitionUnawareFile extends AbstractInputOutputFile
{
    private final Path path;

    public PartitionUnawareFile(OutputFormat outputFormat, String path)
    {
        this(outputFormat, Paths.get(path), 1);
    }

    private PartitionUnawareFile(OutputFormat outputFormat, Path path, int number)
    {
        super(outputFormat, path, number);
        this.path = resolvePath();
    }

    @Override
    public PartitionUnawareFile next()
    {
        return new PartitionUnawareFile(getOutputFormat(), getInternalPath(), nextNumber());
    }

    @Override
    public String getPath()
    {
        return path.toAbsolutePath().toString();
    }

    @Override
    public String toString()
    {
        return path.toAbsolutePath().toString();
    }

    private Path resolvePath()
    {
        return Paths.get(getInternalPath().toAbsolutePath()
                                 .toString()
                                 .replace(getOutputFormat().getFileExtension(),
                                          "-" + getNumber() + getOutputFormat().getFileExtension()));
    }
}
