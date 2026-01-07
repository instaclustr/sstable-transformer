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
import org.apache.cassandra.spark.data.LocalDataLayer;

public class LocalDataLayerWrapper extends DataLayerWrapper
{
    private final LocalDataLayer dataLayer;

    /**
     *
     * @param dataLayer local data layer
     * @param destination initial destination to write data to.
     * @param maxRowsPerBatch maximum number of rows an individual batch to process can contain
     */
    public LocalDataLayerWrapper(LocalDataLayer dataLayer, PartitionUnawareFile destination, long maxRowsPerBatch)
    {
        super(destination, maxRowsPerBatch);
        this.dataLayer = dataLayer;
    }

    @Override
    public DataLayer getDataLayer()
    {
        return dataLayer;
    }

    @Override
    public int getPartition()
    {
        // partition 0 as local data layer does not have concept of partitions (has one partition)
        return 0;
    }
}
