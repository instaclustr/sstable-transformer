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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Mixin;

import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

import static com.instaclustr.transformer.core.DataLayerHelpers.getDataLayerTransformers;

@Command(name = "transform",
        description = "Transform SSTables to Parquet or Avro files.",
        versionProvider = Transformer.class,
        subcommands = HelpCommand.class)
public class SSTableTransformer implements Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(SSTableTransformer.class);

    @Mixin
    private TransformerOptions options;

    // for picocli
    public SSTableTransformer()
    {
    }

    public SSTableTransformer(TransformerOptions options)
    {
        this.options = options;
        options.validate();
    }

    @Override
    public void run()
    {
        options.validate();
        for (Object individualResult : runTransformation())
            logger.info(individualResult.toString());
    }

    /**
     * Runs transformation. Used for programmatic invocation, e.g. via Spark job.
     *
     * @return list of files which were created as part of transformation
     */
    public List<Object> runTransformation(ServiceLoader.Provider<TransformationSink> sinkProvider)
    {
        List<DataLayerTransformer> dataLayerTransformers = getDataLayerTransformers(options, sinkProvider);

        if (!dataLayerTransformers.isEmpty())
        {
            try (TransformationExecutor executor = new TransformationExecutor(options.parallelism))
            {
                logger.info("Running transformation with parallelism " + options.parallelism);
                return executor.run(dataLayerTransformers);
            }
        }
        else
        {
            logger.info("Nothing to transform. Check paths to input directories / files are correct.");
            return Collections.emptyList();
        }
    }

    /**
     *
     * @param transformationSinkClass class to create new instances from
     * @return transformation result
     */
    public List<Object> runTransformation(Class<? extends TransformationSink> transformationSinkClass)
    {
        if (transformationSinkClass == null)
            throw new TransformerException("sink class can not be null!");

        return runTransformation(new TransformationSinkProvider(transformationSinkClass));
    }

    /**
     * Runs transformation without any sinks.
     *
     * @return transformation result
     */
    public List<Object> runTransformation()
    {
        if (!options.hasSink())
        {
            return runTransformation((ServiceLoader.Provider<TransformationSink>) null);
        }
        else
            return runTransformation(SPISinkProvider.getSinkProvider(options.sinkName()));
    }
}
