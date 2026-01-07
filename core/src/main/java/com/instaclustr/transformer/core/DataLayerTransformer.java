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

import java.util.List;
import java.util.ServiceLoader;

import static java.lang.String.format;

/**
 * This class is responsible for taking an instance of {@link DataLayerWrapper}, being it
 * local or remote, and transforming data it is responsible for (to files or other means) by iterating over
 * all rows by {@link DataLayerReader}.
 */
public class DataLayerTransformer
{
    private final TransformerOptions options;
    private final DataLayerWrapper dataLayerWrapper;
    private final ServiceLoader.Provider<TransformationSink> sinkProvider;

    public DataLayerTransformer(TransformerOptions options,
                                DataLayerWrapper dataLayerWrapper)
    {
        this(options, dataLayerWrapper, null);
    }

    public DataLayerTransformer(TransformerOptions options,
                                DataLayerWrapper dataLayerWrapper,
                                ServiceLoader.Provider<TransformationSink> sinkProvider)
    {
        this.options = options;
        this.dataLayerWrapper = dataLayerWrapper;
        this.sinkProvider = sinkProvider;
    }

    /**
     * Transforms data.
     *
     * @return list of transformation results, for file based transformations it is list of created files.
     */
    public List<Object> transform()
    {
        return transform(sinkProvider);
    }

    /**
     * Transforms data via given sink.
     *
     * @param sinkProvider sink provider to use, optional, can be null
     * @return list of transformation results, for file based transformations it is list of created files.
     */
    public List<Object> transform(ServiceLoader.Provider<TransformationSink> sinkProvider)
    {
        TransformationSink sink = null;

        if (sinkProvider != null)
            sink = sinkProvider.get();

        TransformationSink.validate(sink, options.outputFormat);

        try
        {
            if (sink != null)
                sink.init(options.sinkConfig);
        }
        catch (Exception ex)
        {
            throw new TransformerException(format("Unable to validate and initialize sink '%s': %s",
                                                  options.sinkName(),
                                                  ex.getMessage()));
        }

        try (DataLayerReader reader = new DataLayerReader(dataLayerWrapper, options, sink))
        {
            return reader.read();
        }
        catch (Throwable t)
        {
            throw new TransformerException("Unable to transform to " + options.outputFormat, t);
        }
    }
}
