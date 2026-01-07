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
import org.apache.cassandra.clients.Sidecar;
import org.apache.cassandra.secrets.SslConfig;
import org.apache.cassandra.spark.data.CassandraDataLayer;
import org.apache.cassandra.spark.data.ClientConfig;

import javax.annotation.Nullable;
import java.math.BigInteger;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

/**
 * This data layer will internally construct {@link CassandraDataLayer} which will read data from Cassandra remotely
 * via Sidecars.
 */
public class RemoteDataLayersBuilder extends AbstractDataLayersBuilder
{
    private final CassandraDataLayer dataLayer;
    private final String output;
    private final List<Integer> partitions;
    private final OutputFormat outputFormat;

    /**
     * @param clientConfigMap        configuration for client
     * @param sidecarClientConfigMap configuration for sidecars
     * @param sslSecretsConfigMap    configuration for SSL for sidecars
     * @param options                options
     * @param partition              Spark partition to transform
     */
    public RemoteDataLayersBuilder(Map<String, String> clientConfigMap,
                                   Map<String, String> sidecarClientConfigMap,
                                   @Nullable Map<String, String> sslSecretsConfigMap,
                                   TransformerOptions options,
                                   int partition)
    {
        this(clientConfigMap,
             sidecarClientConfigMap,
             sslSecretsConfigMap,
             options,
             List.of(partition));
    }

    /**
     * @param clientConfigMap        configuration for client
     * @param sidecarClientConfigMap configuration for sidecars
     * @param sslSecretsConfigMap    configuration for SSL for sidecars
     * @param options                options
     * @param partitions             Spark partitions to transform
     */
    public RemoteDataLayersBuilder(Map<String, String> clientConfigMap,
                                   Map<String, String> sidecarClientConfigMap,
                                   @Nullable Map<String, String> sslSecretsConfigMap,
                                   TransformerOptions options,
                                   List<Integer> partitions)
    {
        super(options.maxRowsPerFile);
        ClientConfig clientConf = ClientConfig.create(clientConfigMap);
        Sidecar.ClientConfig sidecarClientConfig = Sidecar.ClientConfig.create(sidecarClientConfigMap);
        SslConfig sslConfig = sslSecretsConfigMap != null ? SslConfig.create(sslSecretsConfigMap) : SslConfig.create(Map.of());
        CassandraDataLayer dataLayer = new CassandraDataLayer(clientConf, sidecarClientConfig, sslConfig);
        dataLayer.initialize(clientConf);
        this.dataLayer = dataLayer;
        this.output = options.output;
        this.outputFormat = options.outputFormat;
        this.partitions = partitions.isEmpty() ? getDataLayerPartitions() : partitions;
        validatePartitions();
    }

    /**
     * @return wrappers, one per partition.
     */
    public List<RemoteDataLayerWrapper> build()
    {
        return partitions.stream().map(partition ->
                                       {
                                           Range<BigInteger> tokenRange = dataLayer.tokenPartitioner().getTokenRange(partition);
                                           return new RemoteDataLayerWrapper(dataLayer, partition, getOutputFile(output, tokenRange, partition), maxRowsPerBatch());
                                       }).collect(toList());
    }

    private List<Integer> getDataLayerPartitions()
    {
        return IntStream.rangeClosed(0, dataLayer.partitionCount() - 1).boxed().collect(toList());
    }

    private void validatePartitions()
    {
        Set<Integer> dataLayerPartitions = new HashSet<>(getDataLayerPartitions());
        List<Integer> missingPartitions = new ArrayList<>();
        for (int currentPartition : this.partitions)
        {
            if (!dataLayerPartitions.contains(currentPartition))
                missingPartitions.add(currentPartition);
        }

        if (!missingPartitions.isEmpty())
            throw new IllegalStateException("There are no partitions " + missingPartitions + " in initialized data layer.");
    }

    private PartitionAwareFile getOutputFile(String output, Range<BigInteger> tokenRange, int partition)
    {
        Path outputPath = Paths.get(output);
        String fileExtension = outputFormat.getFileExtension();
        if (outputPath.toFile().isDirectory())
            return new PartitionAwareFile(outputFormat, outputPath.resolve(UUID.randomUUID() + fileExtension), tokenRange, partition, 0);
        else
            return new PartitionAwareFile(outputFormat, outputPath, tokenRange, partition, 0);
    }
}
