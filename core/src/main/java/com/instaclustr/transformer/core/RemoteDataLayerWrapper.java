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

import org.apache.cassandra.spark.data.CassandraDataLayer;
import org.apache.cassandra.spark.data.DataLayer;

/**
 * Wrapper for reading Cassandra's data remotely - when the host we transform
 * the data on is different from the host Cassandra node runs on. The data
 * are read by Sidecar co-located on Cassandra's host, and they are serialized transparently to us
 * and iterated over by {@link org.apache.cassandra.spark.sparksql.SparkRowIterator}.
 */
public class RemoteDataLayerWrapper extends DataLayerWrapper
{
    private final CassandraDataLayer dataLayer;
    private final int partition;

    /**
     * @param dataLayer       data layer to work with
     * @param partition       logical Spark partition to read data from - backed by Cassandra ranges
     * @param destination     destination to write data to
     * @param maxRowsPerBatch max number of rows per one batch to process
     */
    public RemoteDataLayerWrapper(CassandraDataLayer dataLayer,
                                  int partition,
                                  PartitionAwareFile destination,
                                  long maxRowsPerBatch)
    {
        super(destination, maxRowsPerBatch);
        this.dataLayer = dataLayer;
        this.partition = partition;
    }

    @Override
    public DataLayer getDataLayer()
    {
        return dataLayer;
    }

    @Override
    public int getPartition()
    {
        return partition;
    }
}
