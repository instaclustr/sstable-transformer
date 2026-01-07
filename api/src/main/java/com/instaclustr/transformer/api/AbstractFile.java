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
package com.instaclustr.transformer.api;

import java.nio.file.Path;

public abstract class AbstractFile
{
    private final OutputFormat outputFormat;
    private final Path internalPath;
    private final int number;
    private volatile boolean finished = false;
    private int count;

    public AbstractFile(OutputFormat outputFormat,
                        Path path,
                        int number)
    {
        this.outputFormat = outputFormat;
        this.internalPath = path;
        this.number = number;
    }

    public OutputFormat getOutputFormat()
    {
        return outputFormat;
    }

    public Path getInternalPath()
    {
        return internalPath;
    }

    public int getNumber()
    {
        return number;
    }

    public int nextNumber()
    {
        return number + 1;
    }

    public abstract String getPath();

    /**
     * Returns logically next output file a transformation process should write data to when this one is considered full.
     *
     * @return next output file.
     */
    public abstract AbstractFile next();

    /**
     * After this method is called, no rows should be written to it anymore.
     */
    public void finish()
    {
        finished = true;
    }

    /**
     * A file is considered to be eligible to be written to when it is not finished.
     * <p>
     * See {@link AbstractFile#finished}.
     *
     * @return true if we can write to this file, false otherwise.
     */
    public boolean canWrite()
    {
        return !finished;
    }

    /**
     * Sets number of rows this file contains.
     *
     * @param rows number of rows this file contains
     */
    public void setCount(int rows)
    {
        this.count = rows;
    }

    /**
     * Returns number of rows this file contains.
     *
     * @return number of rows this file contains.
     */
    public int getCount()
    {
        return count;
    }
}
