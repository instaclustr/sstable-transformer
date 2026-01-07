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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

/**
 * Serves as an executor of each {@link DataLayerTransformer}.
 * <p>
 * The execution is done in a parallel manner. The default number of cores to execute the transformation on
 * is driven by {@link TransformerOptions#parallelism} configuration option,
 * by default it is equal to number of cores.
 */
public class TransformationExecutor implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(TransformationExecutor.class);

    private final ExecutorService executor;

    public TransformationExecutor(int threads)
    {
        executor = Executors.newFixedThreadPool(threads);
    }

    public List<Object> run(Collection<DataLayerTransformer> transformers)
    {
        return waitForCompletion(submit(transformers));
    }

    private List<Object> waitForCompletion(Collection<CompletableFuture<List<Object>>> futures)
    {
        try
        {
            return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .thenApply(v -> futures.stream().map(CompletableFuture::join).collect(toList()))
                    .get()
                    .stream()
                    .flatMap(Collection::stream)
                    .collect(toList());
        }
        catch (InterruptedException | ExecutionException e)
        {
            throw new TransformerException("Unable to transform data.", e);
        }
    }

    private Collection<CompletableFuture<List<Object>>> submit(Collection<DataLayerTransformer> transformers)
    {
        if (executor.isShutdown())
            throw new IllegalStateException("Executor is shut down");

        logger.info("Submitting " + transformers.size() + " transformers.");
        return transformers.stream().map(t -> CompletableFuture.supplyAsync(t::transform, executor)).collect(toCollection(ArrayList::new));
    }

    @Override
    public void close()
    {
        try
        {
            executor.shutdown();
            if (executor.awaitTermination(1, TimeUnit.MINUTES))
                logger.info("Executor was shut down in a timely and graceful manner.");
            else
            {
                logger.warn("Timeout occurred while shutting down the executor.");
                executor.shutdownNow();
            }
        }
        catch (Throwable ex)
        {
            throw new TransformerException("Exception occurred while shutting down transformation executor", ex);
        }
    }
}
