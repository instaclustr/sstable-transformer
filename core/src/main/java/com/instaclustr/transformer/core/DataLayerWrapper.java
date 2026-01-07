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

import org.apache.cassandra.spark.data.DataLayer;

/**
 * A logical wrapper of {@link DataLayer}.
 */
public abstract class DataLayerWrapper
{
    private AbstractInputOutputFile destination;
    private final long maxRowsPerBatch;

    public DataLayerWrapper(AbstractInputOutputFile destination, long maxRowsPerBatch)
    {
        this.destination = destination;
        this.maxRowsPerBatch = maxRowsPerBatch;
    }

    /**
     * After current file used for writes is considered to be full of data,
     * we need to create a new file to write to. We get the next destination to write to
     * by this method.
     *
     * @return new file to write to
     */
    public AbstractInputOutputFile getNextDestination()
    {
        setNextDestination(currentDestination().next());
        return currentDestination();
    }

    /**
     * @return current destination this wrapper write data to
     */
    public AbstractInputOutputFile currentDestination()
    {
        return destination;
    }

    /**
     * @return maximum number of rows a Parquet file will contain
     * before we switch to another file by {@link DataLayerWrapper#getNextDestination()}
     */
    public long getMaxRowsPerBatch()
    {
        return maxRowsPerBatch;
    }

    /**
     * @param destination destination to write to
     */
    public void setNextDestination(AbstractInputOutputFile destination)
    {
        this.destination = destination;
    }

    public abstract DataLayer getDataLayer();

    /**
     * @return Spark partition this wrapper is responsible for transformation
     */
    public abstract int getPartition();
}
