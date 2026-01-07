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

import com.instaclustr.transformer.api.TransformationSink;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader.Provider;

import static java.util.stream.Collectors.toList;

public class DataLayerHelpers
{
    public static List<DataLayerTransformer> getDataLayerTransformers(TransformerOptions options, Provider<TransformationSink> sinkProvider)
    {
        return getDataLayerWrappers(options).stream().map(w -> new DataLayerTransformer(options, w, sinkProvider)).collect(toList());
    }

    public static List<DataLayerTransformer> getDataLayerTransformers(TransformerOptions options)
    {
        return getDataLayerTransformers(options, SPISinkProvider.getSinkProvider(options.sinkName()));
    }

    public static Collection<? extends DataLayerWrapper> getDataLayerWrappers(TransformerOptions options)
    {
        TransformerOptions.DataLayerLocation dataLayerLocation = options.resolveDataLayerLocation();

        if (dataLayerLocation == TransformerOptions.DataLayerLocation.LOCAL)
            return getLocalDataLayers(options);
        else if (dataLayerLocation == TransformerOptions.DataLayerLocation.REMOTE)
            return getRemoteDataLayers(options);
        else
            throw new IllegalStateException("Unsupported data layer location " + dataLayerLocation.name());
    }

    private static Collection<LocalDataLayerWrapper> getLocalDataLayers(TransformerOptions options)
    {
        return new LocalDataLayersBuilder(options)
                .add(options.input)
                .strategy(options.transformationStrategy)
                .build();
    }

    private static Collection<RemoteDataLayerWrapper> getRemoteDataLayers(TransformerOptions options)
    {
        if (options.sidecar.isEmpty())
            throw new TransformerException("Sidecar list can not be empty for remote reading.");

        Map<String, String> client = options.forRemoteDataLayer();
        Map<String, String> sidecarClient = Map.of("sidecar_port", options.extractPortOfFirstSidecar());
        Map<String, String> ssl = Map.of();

        return new RemoteDataLayersBuilder(client,
                                           sidecarClient,
                                           ssl,
                                           options,
                                           TransformerOptions.parsePartitions(options.partitions)).build();
    }
}
