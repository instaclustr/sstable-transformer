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
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.ParameterException;
import picocli.CommandLine.Spec;

@Command(name = "transformer",
        mixinStandardHelpOptions = true,
        description = "Transforms Cassandra SSTable to Parquet or Avro file",
        subcommands = {SSTableTransformer.class, CassandraPartitionsResolver.class, HelpCommand.class},
        versionProvider = Transformer.class,
        usageHelpWidth = 128)
public class Transformer implements IVersionProvider, Runnable
{
    private static final Logger logger = LoggerFactory.getLogger(Transformer.class);

    @Spec
    private CommandSpec spec;

    public static void main(String[] args)
    {
        System.exit(runTransformation(args));
    }

    /**
     * We do not want to always exit underlying JVM, e.g. in case
     * the code is executed in the context of a Spark job or similar.
     *
     * @param options program options
     * @return exit code
     */
    public static int runTransformation(TransformerOptions options)
    {
        return runTransformation(options.asArgs());
    }

    /**
     * We do not want to always exit underlying JVM, e.g. in case
     * the code is executed in the context of a Spark job or similar.
     *
     * @param args program arguments as for normal invocation
     * @return exit code
     */
    public static int runTransformation(String[] args)
    {
        return new CommandLine(new Transformer())
                .setExecutionExceptionHandler(new TransformerExceptionHandler())
                .setExitCodeExceptionMapper(new ExitCodeMapper())
                .execute(args);
    }

    @Override
    public String[] getVersion() throws Exception
    {
        return VersionParser.parse();
    }

    @Override
    public void run()
    {
        throw new ParameterException(spec.commandLine(), "Missing required parameter.");
    }

    private static class ExitCodeMapper implements CommandLine.IExitCodeExceptionMapper
    {
        @Override
        public int getExitCode(Throwable throwable)
        {
            return 1;
        }
    }

    private static class TransformerExceptionHandler implements CommandLine.IExecutionExceptionHandler
    {
        @Override
        public int handleExecutionException(Exception e, CommandLine commandLine, CommandLine.ParseResult parseResult)
        {
            logger.error("Error while transforming: " + e.getMessage(), e);
            commandLine.usage(System.err);
            return commandLine.getCommandSpec().exitCodeOnExecutionException();
        }
    }
}
