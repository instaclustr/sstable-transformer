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
import org.apache.cassandra.spark.data.FileType;
import org.apache.cassandra.spark.data.LocalDataLayer;
import org.apache.cassandra.spark.utils.Throwing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.stream.Collectors.toSet;

/**
 * The meaning of a local data layer is that it is reading data from SSTables directly from a local disk.
 * It does not have any concept of partitions.
 */
public class LocalDataLayersBuilder extends AbstractDataLayersBuilder
{
    private static final Logger logger = LoggerFactory.getLogger(LocalDataLayersBuilder.class);

    private final TransformerOptions options;
    private TransformerOptions.TransformationStrategy transformationStrategy = TransformerOptions.TransformationStrategy.ONE_FILE_PER_SSTABLE;
    private final Set<Path> sstableDataPaths = new HashSet<>();
    private final String output;
    private final Map<String, String> dataLayerOptions;

    /**
     * @param options program options
     */
    public LocalDataLayersBuilder(TransformerOptions options)
    {
        super(options.maxRowsPerFile);
        Map<String, String> localDataLayerOptions = options.forLocalDataLayer();
        if (localDataLayerOptions.get("version") == null)
            throw new TransformerException("Missing 'version' option");
        if (localDataLayerOptions.get("partitioner") == null)
            throw new TransformerException("Missing 'partitioner' option");
        if (localDataLayerOptions.get("keyspace") == null)
            throw new TransformerException("Missing 'keyspace' option");
        if (localDataLayerOptions.get("createstmt") == null)
            throw new TransformerException("Missing 'createstmt' option");

        this.options = options;
        this.dataLayerOptions = new HashMap<>(localDataLayerOptions);
        this.output = options.output;
    }

    /**
     * Sets appropriate strategy we will transform data with.
     *
     * @param transformationStrategy strategy to set
     * @return this builder.
     */
    public LocalDataLayersBuilder strategy(TransformerOptions.TransformationStrategy transformationStrategy)
    {
        if (transformationStrategy != null)
            this.transformationStrategy = transformationStrategy;
        return this;
    }

    public LocalDataLayersBuilder add(String... paths)
    {
        return add(Arrays.stream(paths).map(Paths::get).collect(toSet()));
    }

    public LocalDataLayersBuilder add(Path... paths)
    {
        return add(Arrays.stream(paths).collect(toSet()));
    }

    public LocalDataLayersBuilder add(List<String> paths)
    {
        return add(paths.stream().filter(Objects::nonNull).map(Paths::get).collect(toSet()));
    }

    /**
     * Paths can be a collection of individual files or directories, mixed together.
     * If a directory is encountered, it is listed and each file filtered to see if it is Cassandra's {@code -Data.db}
     * file. If a file is encountered, the same test is conducted. Each file to be processed has to exist and has to
     * be readable.
     *
     * @param paths path to read data from
     * @return built data layer which will be reading data from given path.
     */
    public LocalDataLayersBuilder add(Collection<Path> paths)
    {
        Set<Path> sanitizedPaths = paths.stream().filter(Objects::nonNull).collect(toSet());

        for (Path path : sanitizedPaths)
        {
            File dirOrFile = path.toFile();
            if (dirOrFile.isDirectory())
            {
                try
                {
                    Set<Path> dataFiles = Throwing.function(Files::list)
                            .apply(path)
                            .filter(this::isDataFile)
                            .filter(p ->
                                    {
                                        if (p.toFile().canRead())
                                        {
                                            return true;
                                        } else
                                        {
                                            logger.info("Skipping not readable {} from processing.", p.toAbsolutePath());
                                            return false;
                                        }
                                    })
                            .collect(toSet());

                    sstableDataPaths.addAll(dataFiles);
                } catch (Throwable t)
                {
                    throw new TransformerException(format("Unable to list %s", path.toFile().getAbsolutePath()), t);
                }
            } else
            {
                if (isDataFile(path))
                {
                    if (!dirOrFile.exists() || !dirOrFile.canRead())
                    {
                        logger.info("Skipping non-existing or non-readable {} from processing.", dirOrFile.getAbsolutePath());
                        continue;
                    }

                    sstableDataPaths.add(path);
                }
            }
        }
        return this;
    }

    /**
     * Builds local data layer wrappers. Based on used strategy, the resulting collection might
     * contain only one wrapper when {@link TransformerOptions.TransformationStrategy#ONE_FILE_ALL_SSTABLES} is used,
     * or multiple wrappers when {@link TransformerOptions.TransformationStrategy#ONE_FILE_PER_SSTABLE} is used.
     *
     * @return wrappers
     */
    public Collection<LocalDataLayerWrapper> build()
    {
        if (sstableDataPaths.isEmpty())
            return Collections.emptyList();

        switch (transformationStrategy)
        {
            case ONE_FILE_ALL_SSTABLES:
                return oneParquetAllSSTables();
            case ONE_FILE_PER_SSTABLE:
                return oneParquetPerSSTable();
            default:
                throw new TransformerException(format("Unsupported transformation strategy %s", transformationStrategy));
        }
    }

    private boolean isDataFile(Path maybeDataFile)
    {
        return maybeDataFile.getFileName().toString().endsWith("-" + FileType.DATA.getFileSuffix());
    }

    private Collection<LocalDataLayerWrapper> oneParquetPerSSTable()
    {
        List<LocalDataLayerWrapper> wrappedDataLayers = new ArrayList<>();

        for (Path dataPath : sstableDataPaths)
        {
            LocalDataLayer dataLayer = LocalDataLayer.from(dataLayerOptions);
            dataLayer.setDataFilePaths(Set.of(dataPath));
            wrappedDataLayers.add(new LocalDataLayerWrapper(dataLayer, getOutputFile(output, dataPath), maxRowsPerBatch()));
        }

        return wrappedDataLayers;
    }

    private Collection<LocalDataLayerWrapper> oneParquetAllSSTables()
    {
        LocalDataLayer dataLayer = LocalDataLayer.from(dataLayerOptions);
        dataLayer.setDataFilePaths(new HashSet<>(sstableDataPaths));

        PartitionUnawareFile file = options.outputFormat == OutputFormat.ARROW_STREAM ? null : getOutputFile(output, null);

        return Collections.singletonList(new LocalDataLayerWrapper(dataLayer,
                                                                   file,
                                                                   maxRowsPerBatch()));
    }


    /**
     * Returns path, as String, to resulting Parquet file a SSTable will be transformed to.
     * <p>
     * SSTable will be transformed into a file with {@code .parquet} suffix and with name equal to original Data
     * component of SSTable being transformed if {@code output} is {@code null}.
     * <p>
     * If {@code output} is not null, Parquet file under that path will be used.
     * If {@code output} is not null nor {@code dataPath} is, then the resulting Parquet file will
     * be created under same rules as described above, and it will be created in a directory denoted by {@code output}.
     *
     * @param output   user defined path, when null, dataPath will be used.
     * @param dataPath data path of SSTable DATA component.
     * @return resulting Parquet's OutputFile to write to
     */
    private PartitionUnawareFile getOutputFile(String output, Path dataPath)
    {
        if (output == null && dataPath == null)
            throw new TransformerException("both output and data path can not be null at the same time");

        OutputFormat format = options.outputFormat;
        String fileExtension = format.fileExtension;

        if (output == null)
            return new PartitionUnawareFile(format,
                                            dataPath.toAbsolutePath() + fileExtension);

        Path outputPath = Paths.get(output);

        if (dataPath != null)
        {
            String aFile = dataPath.getFileName().toString() + fileExtension;
            return new PartitionUnawareFile(format, outputPath.resolve(aFile).toAbsolutePath().toString());
        } else
        {
            if (outputPath.toFile().isDirectory())
                return new PartitionUnawareFile(format, outputPath.resolve(UUID.randomUUID() + fileExtension).toAbsolutePath().toString());
            else
                return new PartitionUnawareFile(format, output);
        }
    }
}
