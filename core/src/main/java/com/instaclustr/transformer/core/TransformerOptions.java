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
import org.apache.avro.file.CodecFactory;
import org.apache.cassandra.bridge.CassandraVersion;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Option;

import java.io.FileInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.instaclustr.transformer.core.TransformerOptions.TransformationStrategy.ONE_FILE_ALL_SSTABLES;
import static com.instaclustr.transformer.core.TransformerOptions.TransformationStrategy.ONE_FILE_PER_SSTABLE;

public class TransformerOptions implements Serializable
{
    private static final Logger logger = LoggerFactory.getLogger(TransformerOptions.class);

    private static final Pattern CREATE_TABLE_PATTERN = Pattern.compile(
            "CREATE\\s+TABLE\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?" +
            "(?:\"([^\"]+)\"|([a-zA-Z][a-zA-Z0-9_]*))\\..*",
            Pattern.CASE_INSENSITIVE
    );

    public enum TransformationStrategy implements Serializable
    {
        ONE_FILE_ALL_SSTABLES,
        ONE_FILE_PER_SSTABLE
    }

    public enum DataLayerLocation implements Serializable
    {
        LOCAL,
        REMOTE
    }

    public enum Compression
    {
        UNCOMPRESSED
                {
                    @Override
                    CodecFactory forAvro()
                    {
                        return CodecFactory.nullCodec();
                    }

                    @Override
                    CompressionCodecName forParquet()
                    {
                        return CompressionCodecName.UNCOMPRESSED;
                    }
                },
        SNAPPY
                {
                    @Override
                    CodecFactory forAvro()
                    {
                        return CodecFactory.snappyCodec();
                    }

                    @Override
                    CompressionCodecName forParquet()
                    {
                        return CompressionCodecName.SNAPPY;
                    }
                },
        ZSTD
                {
                    @Override
                    CodecFactory forAvro()
                    {
                        return CodecFactory.zstandardCodec(CodecFactory.DEFAULT_ZSTANDARD_LEVEL, true);
                    }

                    @Override
                    CompressionCodecName forParquet()
                    {
                        return CompressionCodecName.ZSTD;
                    }
                };

        abstract CodecFactory forAvro();

        abstract CompressionCodecName forParquet();
    }

    /**
     * Used. e.g. in Spark jobs for programmatic creation of options.
     */
    public static class Builder implements Serializable
    {
        private OutputFormat outputFormat = OutputFormat.PARQUET;
        private String keyspace;
        private String table;
        private String createTableStmt;
        private final List<String> input = new ArrayList<>();
        private String output;
        private final Set<String> sidecars = new HashSet<>();
        private String partitions;
        private Compression compression = Compression.ZSTD;
        private boolean bloomFilterEnabled;
        private long maxRowsPerFile = -1;
        private int parallelism = Runtime.getRuntime().availableProcessors();
        private TransformationStrategy transformationStrategy = ONE_FILE_PER_SSTABLE;
        private boolean sorted;
        private Path sinkConfig;

        public Builder outputFormat(OutputFormat outputFormat)
        {
            this.outputFormat = outputFormat;
            return this;
        }

        public Builder keyspace(String keyspace)
        {
            this.keyspace = keyspace;
            return this;
        }

        public Builder table(String table)
        {
            this.table = table;
            return this;
        }

        public Builder createTableStmt(String createTableStmt)
        {
            this.createTableStmt = createTableStmt;
            return this;
        }

        public Builder input(String[] input)
        {
            this.input.addAll(Arrays.asList(input));
            return this;
        }

        public Builder output(String output)
        {
            this.output = output;
            return this;
        }

        public Builder sidecars(String[] sidecars)
        {
            this.sidecars.addAll(Arrays.asList(sidecars));
            return this;
        }

        public Builder sidecar(String sidecar)
        {
            this.sidecars.add(sidecar);
            return this;
        }

        public Builder partitions(String partitions)
        {
            parsePartitions(partitions);
            this.partitions = partitions;
            return this;
        }

        public Builder partition(int partition)
        {
            parsePartitions(Integer.toString(partition));
            this.partitions = Integer.toString(partition);
            return this;
        }

        public Builder compression(Compression compression)
        {
            this.compression = compression;
            return this;
        }

        public Builder bloomFilterEnabled(boolean bloomFilterEnabled)
        {
            this.bloomFilterEnabled = bloomFilterEnabled;
            return this;
        }

        public Builder maxRowsPerFile(long maxRowsPerFile)
        {
            this.maxRowsPerFile = maxRowsPerFile;
            return this;
        }

        public Builder parallelism(int parallelism)
        {
            if (parallelism < 1)
                throw new IllegalArgumentException("Number of threads can not be lower than 1.");
            this.parallelism = parallelism;
            return this;
        }

        public Builder transformationStrategy(TransformationStrategy transformationStrategy)
        {
            this.transformationStrategy = transformationStrategy;
            return this;
        }

        public Builder sorted(boolean sorted)
        {
            this.sorted = sorted;
            return this;
        }

        public Builder sinkConfig(Path sinkConfig)
        {
            this.sinkConfig = sinkConfig;
            return this;
        }

        public TransformerOptions build()
        {
            TransformerOptions options = new TransformerOptions();

            options.outputFormat = outputFormat;
            options.keyspace = keyspace;
            options.table = table;
            options.createTableStmt = createTableStmt;
            options.input = input;
            options.output = output;
            options.sidecar = new ArrayList<>(sidecars);
            options.compression = compression;
            options.bloomFilterEnabled = bloomFilterEnabled;
            options.maxRowsPerFile = maxRowsPerFile;
            options.partitions = partitions;
            options.parallelism = parallelism;
            options.transformationStrategy = transformationStrategy;
            options.sorted = sorted;
            options.sinkConfig = sinkConfig;

            options.validate();

            return options;
        }
    }

    // for now only this version is supported
    @Option(names = "--cassandra-version",
            description = "FIVEZERO or FOURZERO, defaults to FIVEZERO")
    public CassandraVersion cassandraVersion = CassandraVersion.FIVEZERO;
    // for now only this partitioner is supported
    public Partitioner partitioner = Partitioner.Murmur3Partitioner;

    @Option(names = "--output-format",
            description = "Output format of data, either AVRO or PARQUET")
    public OutputFormat outputFormat = OutputFormat.PARQUET;

    @Option(names = {"--keyspace"},
            description = "Cassandra keyspace name. You do not need to specify it " +
                    "for local data layers.")
    public String keyspace;

    @Option(names = {"--table"},
            description = "Cassandra table name. You do not need to specify it for local data layer.")
    public String table;

    @Option(names = {"--create-table-statement"},
            description = "CQL statement as for table creation. You do not need to specify it for remote data layer.")
    public String createTableStmt;

    @Option(names = {"--input"},
            description = "List of directories or individual files to transform. " +
                    "Directories can be mixed with files. You do not need to specify it if you specify --sidecar",
            arity = "1..*")
    public List<String> input = new ArrayList<>();

    @Option(names = {"--output"},
            description = "Output file or destination, relevant when not using ARROW_STREAM format.")
    public String output;

    @Option(names = {"--sidecar"},
            description = "List of sidecar hostnames with ports.",
            arity = "1..*")
    public List<String> sidecar = new ArrayList<>();

    @Option(names = {"--compression"},
            description = "Use compression for output files, it can be UNCOMPRESSED, SNAPPY, ZSTD.")
    public Compression compression = Compression.ZSTD;

    @Option(names = {"--bloom-filter"},
            description = "Flag for telling whether bloom filter should be used upon writing of a Parquet file.")
    public boolean bloomFilterEnabled;

    @Option(names = {"--max-rows-per-file"},
            description = "Maximal number of rows per file. Has to be bigger than 0. Defaults to " +
                    "undefined which will put all rows to one file.")
    public long maxRowsPerFile = -1;

    @Option(names = {"--partitions"},
            description = "Spark partitions to process. Can be a number, a range (n..m), or enumeration (1,2,3...). " +
                    "Defaults to all partitions.")
    public String partitions;

    @Option(names = {"--parallelism"},
            description = "Number of transformation tasks to run simultaneously. Defaults to number of processors.")
    public int parallelism = Runtime.getRuntime().availableProcessors();

    @Option(names = {"--strategy"},
            description = "Whether to convert all SSTables into one file " +
                    "or there will be one output file per SSTable. " +
                    "Can be one of ONE_FILE_PER_SSTABLE, ONE_FILE_ALL_SSTABLES. " +
                    "Defaults to ONE_FILE_PER_SSTABLE - can not be used when --sidecar is specified.")
    public TransformationStrategy transformationStrategy = ONE_FILE_PER_SSTABLE;

    @Option(names = {"--sorted"},
            description = "Flag for telling whether rows in each file should be sorted or not. " +
                    "Use with caution as sorting will happen in memory and all Spark rows will be held in memory " +
                    "until sorting is done. For large datasets, use this flag together with --max-rows-per-file so " +
                    "sorting will be limited to number of rows per that option only.")
    public boolean sorted;

    @Option(names = {"--keep-snapshot"},
            description = "Flag for telling whether we should keep snapshot used for remote transformation.")
    public boolean keepSnapshot;

    @Option(names = {"--sink-config"},
            description = "Configuration file for a sink. Each sink is responsible for parsing its content.")
    public Path sinkConfig;

    public Properties sinkConfigProperties;

    public String sinkName()
    {
        if (sinkConfigProperties == null)
            throw new IllegalStateException("There is no --sink-config, can not determine sink name!");

        return sinkConfigProperties.getProperty("name");
    }

    public boolean hasSink()
    {
        return sinkConfigProperties != null && sinkConfigProperties.getProperty("name") != null;
    }

    public Map<String, String> forRemoteDataLayer()
    {
        return Map.of("sidecar_contact_points", parseSidecars(),
                      "keyspace", keyspace,
                      "table", table,
                      "createsnapshot", "true",
                      "clearsnapshot", Boolean.toString(!keepSnapshot));
    }

    public String extractPortOfFirstSidecar()
    {
        String allContactPoints = forRemoteDataLayer().get("sidecar_contact_points");
        String firstSidecar = allContactPoints.split(",")[0];
        String[] split = firstSidecar.split(":");
        return split.length == 2 ? split[1] : "9043";
    }

    public Map<String, String> forLocalDataLayer()
    {
        Map<String, String> map = new HashMap<>();
        map.put("version", cassandraVersion.name());
        map.put("partitioner", partitioner.name());
        map.put("keyspace", parseKeyspace(createTableStmt));
        map.put("createstmt", createTableStmt);
        // just to have something, we will populate individual SSTables anyway
        map.put("dirs", "");
        return map;
    }

    public static String parseKeyspace(String createTableStmt)
    {
        Matcher matcher = CREATE_TABLE_PATTERN.matcher(createTableStmt);
        if (matcher.matches())
        {
            String quoted = matcher.group(1);
            return quoted != null ? quoted : matcher.group(2);
        }
        else
            throw new IllegalStateException("Unable to parse keyspace from create table statement: " + createTableStmt);
    }

    public String parseSidecars()
    {
        return String.join(",", sidecar);
    }

    /**
     * Partitions might be in the form of:
     * <p>
     * a) "1" - simple integer, one partition
     * b) "1..10" - all partitions from 1 to 10, inclusive
     * c) "1,2,3,4,5" - explicit enumeration of partitions (generalisation of a))
     * d) no explicit partition - whole token ring will be processed
     *
     * @param partitions partitions to parse
     * @return parsed list of partitions
     */
    public static List<Integer> parsePartitions(String partitions)
    {
        if (partitions == null || partitions.isBlank())
            return List.of();

        Set<Integer> result = new LinkedHashSet<>();

        if (partitions.contains(".."))
        {
            IllegalArgumentException exception = new IllegalArgumentException("Range partitions have to be in form of \'n..m\' where n, m are numbers and n < m.");

            String[] split = partitions.split("\\.\\.");

            if (split.length != 2)
                throw exception;

            int n;
            int m;

            try
            {
                n = Integer.parseInt(split[0].trim());
                m = Integer.parseInt(split[1].trim());
            } catch (Throwable t)
            {
                throw exception;
            }

            if (n >= m)
                throw exception;

            IntStream.rangeClosed(n, m).boxed().collect(Collectors.toCollection(() -> result));
        } else
        {
            IllegalArgumentException exception = new IllegalArgumentException("Range partitions have to be in form of \'n,m,l[...],o\'.");
            for (String s : partitions.split(","))
            {
                try
                {
                    result.add(Integer.parseInt(s.trim()));
                } catch (Throwable t)
                {
                    throw exception;
                }
            }
        }

        if (!result.stream().allMatch(partition -> partition >= 0))
            throw new IllegalArgumentException("All partitions have to be positive numbers");

        return result.stream().sorted().collect(Collectors.toList());
    }

    public void validate()
    {
        if (!input.isEmpty() && !sidecar.isEmpty())
        {
            throw new IllegalArgumentException("You have specified --input to read SSTables from local disk," +
                                                       "but you can not specify sidecar instances to read remotely as well. " +
                                                       "Choose only one.");
        }

        if (input.isEmpty() && sidecar.isEmpty())
        {
            throw new IllegalArgumentException("You have not specified --sidecar to read SSTables remotely nor " +
                                                       "--input option to read from local disk either. Choose one.");
        }

        if (resolveDataLayerLocation() == DataLayerLocation.REMOTE)
        {
            if (keyspace == null)
                throw new IllegalArgumentException("You have to specify --keyspace for remote data layers");
            if (table == null)
                throw new IllegalArgumentException("You have to specify --table for remote data layers");
        } else if (createTableStmt == null)
        {
            throw new IllegalArgumentException("You have to specify --create-table-statement for local data layers");
        }

        if (output == null)
        {
            if (outputFormat != OutputFormat.ARROW_STREAM)
                throw new IllegalArgumentException("--output has to be specified");
        }

        if (maxRowsPerFile != -1 && maxRowsPerFile < 1)
            throw new IllegalArgumentException("--max-rows-per-file can not be lower than 1");

        if (!sidecar.isEmpty() && transformationStrategy == ONE_FILE_PER_SSTABLE)
        {
            logger.info("Changed transformation strategy to {} as {} can not be used when --sidecar is specified.",
                        ONE_FILE_ALL_SSTABLES, ONE_FILE_PER_SSTABLE);
            transformationStrategy = ONE_FILE_ALL_SSTABLES;
        }

        parsePartitions(partitions);

        if (createTableStmt != null)
            parseKeyspace(createTableStmt);

        if (parallelism < 1)
            throw new IllegalArgumentException("Number of threads can not be lower than 1.");

        if (outputFormat == null)
            throw new IllegalArgumentException("Output format has to be specified.");

        if (outputFormat == OutputFormat.ARROW_STREAM && sinkConfig == null)
            throw new IllegalArgumentException("You have to specify --sink-config if you go to use " + OutputFormat.ARROW_STREAM);

        if (sinkConfig != null)
        {
            if (!Files.isRegularFile(sinkConfig))
                throw new IllegalArgumentException("Sink config file is not a regular file: " + sinkConfig);

            if (!Files.isReadable(sinkConfig))
                throw new IllegalArgumentException("Sink config file is not readable: " + sinkConfig);

            sinkConfigProperties = new Properties();
            try (InputStream fis = new FileInputStream(sinkConfig.toFile()))
            {
                sinkConfigProperties.load(fis);
            }
            catch (Throwable t)
            {
                throw new IllegalArgumentException(String.format("Unable to read sink config of %s. Reason: %s",
                                                                 sinkConfig.toAbsolutePath(),
                                                                 t.getMessage()));
            }

            if (sinkConfigProperties.getProperty("name") == null)
                throw new IllegalArgumentException("There is no 'name' property in sink configuration file " + sinkConfig);
        }
    }

    public DataLayerLocation resolveDataLayerLocation()
    {
        if (!sidecar.isEmpty())
            return DataLayerLocation.REMOTE;
        else
            return DataLayerLocation.LOCAL;
    }

    public String[] asArgs()
    {
        List<String> args = new ArrayList<>();
        args.add("transform");
        if (outputFormat != null)
            args.add("--output-format=" + outputFormat.name());
        if (keyspace != null)
            args.add("--keyspace=" + keyspace);
        if (table != null)
            args.add("--table=" + table);
        if (createTableStmt != null)
            args.add("--create-table-statement=" + createTableStmt);
        if (input != null && !input.isEmpty())
        {
            for (String inputEntry : input)
                args.add("--input=" + inputEntry);
        }
        if (output != null)
            args.add("--output=" + output);
        if (sidecar != null && !sidecar.isEmpty())
        {
            for (String sidecarEntry : sidecar)
                args.add("--sidecar=" + sidecarEntry);
        }
        if (compression != null)
            args.add("--compression=" + compression.name());
        if (bloomFilterEnabled)
            args.add("--bloom-filter");
        if (maxRowsPerFile > 0)
            args.add("--max-rows-per-file=" + maxRowsPerFile);

        args.add("--parallelism=" + parallelism);
        args.add("--strategy=" + transformationStrategy.name());

        if (partitions != null)
            args.add("--partitions=" + partitions);

        if (sorted)
            args.add("--sorted");

        if (keepSnapshot)
            args.add("--keep-snapshot");

        return args.toArray(new String[0]);
    }
}
