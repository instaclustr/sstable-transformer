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
import com.instaclustr.transformer.api.SinkModel;
import com.instaclustr.transformer.api.TransformationSink;
import org.apache.avro.Schema;
import org.apache.cassandra.spark.sparksql.SparkRowIterator;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.RowOrdering;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.lang.System.currentTimeMillis;
import static java.util.Arrays.stream;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * Transforms stream of rows from Cassandra by {@link SparkRowIterator}.
 * <p>
 * As rows are being transformed and written to a respective file, if that file is considered to be full,
 * it will take care of switching the writer to the next output file.
 */
public class DataLayerReader implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(DataLayerReader.class);

    private final DataLayerWrapper dataLayerWrapper;
    private final TransformerOptions options;
    private final TransformationSink transformationSink;

    public DataLayerReader(DataLayerWrapper dataLayerWrapper,
                           TransformerOptions options,
                           TransformationSink transformationSink)
    {
        this.dataLayerWrapper = dataLayerWrapper;
        this.options = options;
        this.transformationSink = transformationSink;
    }

    public List<Object> read()
    {
        try (RowsWriter rowsWriter = resolveRowsWriter())
        {
            rowsWriter.write();
            return rowsWriter.getResults();
        }
    }

    private RowsWriter resolveRowsWriter()
    {
        if (options.outputFormat == OutputFormat.ARROW_STREAM)
        {
            String sinkModelPropertyValue = options.sinkConfigProperties.getProperty("sink_model");
            if (sinkModelPropertyValue == null)
                sinkModelPropertyValue = SinkModel.PIPE.name();

            SinkModel sinkModel = SinkModel.valueOf(sinkModelPropertyValue.toUpperCase());
            if (sinkModel == SinkModel.PIPE)
                return new PipedArrowStreamRowsWriter(dataLayerWrapper, transformationSink, options);
            else if (sinkModel == SinkModel.BYTE_BUFFER)
                return new ArrowStreamRowsWriter(dataLayerWrapper, transformationSink, options);
            else
                throw new UnsupportedOperationException("Unsupported sink model " + sinkModel.name());
        } else
        {
            if (options.sorted)
                return new SortedFileBasedRowsWriter(dataLayerWrapper, transformationSink, options);
            else
                return new UnsortedFileBasedRowsWriter(dataLayerWrapper, transformationSink, options);
        }
    }

    @Override
    public void close()
    {
        if (transformationSink != null)
        {
            try
            {
                transformationSink.close();
            } catch (Throwable t)
            {
                logger.warn("Unable to close transformation sink: " + t.getMessage());
            }
        }
    }

    private static abstract class RowsWriter implements AutoCloseable
    {
        protected final DataLayerWrapper dataLayerWrapper;
        protected final TransformationSink transformationSink;
        protected final TransformerOptions options;
        protected final StructType structType;
        protected AbstractRowWriter rowWriter;
        protected int count = 0;
        protected long start = currentTimeMillis();

        public RowsWriter(DataLayerWrapper dataLayerWrapper,
                          TransformationSink transformationSink,
                          TransformerOptions options)
        {
            this.dataLayerWrapper = dataLayerWrapper;
            this.transformationSink = transformationSink;
            this.options = options;
            structType = this.dataLayerWrapper.getDataLayer().structType();
        }

        public void write()
        {
            try (SparkRowIterator iterator = new SparkRowIterator(dataLayerWrapper.getPartition(),
                                                                  dataLayerWrapper.getDataLayer(),
                                                                  dataLayerWrapper.getDataLayer().structType(),
                                                                  emptyList()))
            {
                internalWrite(iterator);
            } catch (Throwable t)
            {
                throw new TransformerException("Error while reading SSTable rows", t);
            } finally
            {
                close();
            }
        }

        @Override
        public void close()
        {
            if (rowWriter != null)
            {
                try
                {
                    rowWriter.close();
                    rowWriter = null;
                } catch (Throwable t)
                {
                    throw new TransformerException("Error while closing writer.", t);
                }
            }
        }

        protected void executeSink(Object object)
        {
            try
            {
                if (transformationSink != null)
                    transformationSink.sink(object);
            } catch (Throwable t)
            {
                throw new TransformerException("Error while executing transformation sink", t);
            }
        }

        protected abstract void internalWrite(SparkRowIterator iterator) throws IOException;

        protected abstract void record(Object individualResult);

        public abstract List<Object> getResults();
    }

    private static class PipedArrowStreamRowsWriter extends RowsWriter
    {
        private static final Logger logger = LoggerFactory.getLogger(PipedArrowStreamRowsWriter.class);

        private PipedArrowStreamRowWriter arrowStreamInMemoryRowWriter;
        private final PipedOutputStream outputStream;
        private final PipedInputStream inputStream;

        private final ExecutorService sinkExecutor =
                Executors.newSingleThreadExecutor(r ->
                                                  {
                                                      Thread t = new Thread(r, "Arrow-Stream-Sink-Thread");
                                                      t.setDaemon(true);
                                                      return t;
                                                  });

        public PipedArrowStreamRowsWriter(DataLayerWrapper dataLayerWrapper,
                                          TransformationSink transformationSink,
                                          TransformerOptions options)
        {
            super(dataLayerWrapper, transformationSink, options);

            try
            {
                inputStream = new PipedInputStream(10 * 1024 * 1024);
                outputStream = new PipedOutputStream(inputStream);
            } catch (Throwable t)
            {
                throw new TransformerException("Error while creating piped streams for Arrow stream writer", t);
            }
        }

        @Override
        protected void internalWrite(SparkRowIterator iterator) throws IOException
        {
            Future<?> sinkFuture = sinkExecutor.submit(() ->
                                                       {
                                                           try
                                                           {
                                                               executeSink(inputStream);
                                                           } catch (Throwable t)
                                                           {
                                                               try
                                                               {
                                                                   inputStream.close();
                                                               } catch (Throwable ex)
                                                               {
                                                                   logger.error("Error closing pipedIn", t);
                                                               }
                                                               logger.error("Error in sink execution", t);
                                                               throw new TransformerException("Sink execution failed", t);
                                                           }
                                                       });

            arrowStreamInMemoryRowWriter = new PipedArrowStreamRowWriter(structType, outputStream);
            rowWriter = arrowStreamInMemoryRowWriter;

            try
            {
                arrowStreamInMemoryRowWriter.start();

                while (iterator.next())
                {
                    rowWriter.accept(iterator.get());
                    count++;
                }

                arrowStreamInMemoryRowWriter.stop();
            } finally
            {
                try
                {
                    outputStream.close();
                } catch (Throwable t)
                {
                    logger.error("Error closing output stream", t);
                }

                try
                {
                    sinkFuture.get(); // block until sink completes
                } catch (Throwable t)
                {
                    throw new TransformerException("Error while waiting for sink execution to complete", t);
                } finally
                {
                    try
                    {
                        inputStream.close();
                    } catch (Throwable t)
                    {
                        logger.error("Error closing input stream", t);
                    }
                }
            }
        }

        @Override
        protected void record(Object individualResult)
        {
            // there is really nothing to "record" as a result as it is in memory
            // and will be wiped out as soon as it is transformed by a sink somewhere
        }

        @Override
        public List<Object> getResults()
        {
            return Collections.emptyList();
        }

        @Override
        public void close()
        {
            try
            {
                sinkExecutor.shutdown();
                if (!sinkExecutor.awaitTermination(1, TimeUnit.MINUTES))
                {
                    logger.warn("Unable to terminate sink executor on time.");
                }
            } catch (Throwable t)
            {
                logger.warn("Unable to terminate sink executor: " + t.getMessage());
            }
            super.close();
        }
    }

    private static class ArrowStreamRowsWriter extends RowsWriter
    {
        private static final Logger logger = LoggerFactory.getLogger(ArrowStreamRowsWriter.class);

        private ArrowStreamInMemoryRowWriter arrowStreamInMemoryRowWriter;
        private final ByteArrayOutputStream outputStream;
        private final int maxRowsBeforeSink;

        public ArrowStreamRowsWriter(DataLayerWrapper dataLayerWrapper,
                                     TransformationSink transformationSink,
                                     TransformerOptions options)
        {
            super(dataLayerWrapper, transformationSink, options);
            outputStream = new ByteArrayOutputStream(Integer.parseInt(options.sinkConfigProperties.getProperty("buffer_size", "1024")));
            maxRowsBeforeSink = Integer.parseInt(options.sinkConfigProperties.getProperty("max_rows_before_sink", "100000"));
            arrowStreamInMemoryRowWriter = new ArrowStreamInMemoryRowWriter(structType, outputStream);
            rowWriter = arrowStreamInMemoryRowWriter;
        }

        @Override
        protected void internalWrite(SparkRowIterator iterator) throws IOException
        {
            arrowStreamInMemoryRowWriter.start();

            while (iterator.next())
            {
                if (count == maxRowsBeforeSink)
                {
                    switchWriter();
                    count = 0;
                }

                rowWriter.accept(iterator.get());
                count++;
            }

            arrowStreamInMemoryRowWriter.stop();
            if (count != 0)
            {
                executeSink(arrowStreamInMemoryRowWriter.getOutputStream());
            }

            arrowStreamInMemoryRowWriter.close();
        }

        @Override
        protected void record(Object individualResult)
        {
            // there is really nothing to "record" as a result as it is in memory
            // and will be wiped out as soon as it is transformed by a sink somewhere
        }

        @Override
        public List<Object> getResults()
        {
            return Collections.emptyList();
        }

        private void switchWriter() throws IOException
        {
            arrowStreamInMemoryRowWriter.stop();
            executeSink(arrowStreamInMemoryRowWriter.getOutputStream());
            arrowStreamInMemoryRowWriter.close();
            arrowStreamInMemoryRowWriter = new ArrowStreamInMemoryRowWriter(structType, outputStream);
            rowWriter = arrowStreamInMemoryRowWriter;
        }

        private long printDuration(int count, long start, long end)
        {
            logger.info("Transformed {} rows in {} seconds.",
                        count,
                        (end - start) / 1000);

            // new start
            return currentTimeMillis();
        }
    }

    private static abstract class FileBasedRowsWriter extends RowsWriter
    {
        private static final Logger logger = LoggerFactory.getLogger(FileBasedRowsWriter.class);

        private final List<AbstractInputOutputFile> outputFiles = new ArrayList<>();
        protected final Schema avroSchema;

        protected FileBasedRowsWriter(DataLayerWrapper dataLayerWrapper,
                                      TransformationSink transformationSink,
                                      TransformerOptions options)
        {
            super(dataLayerWrapper, transformationSink, options);

            record(dataLayerWrapper.currentDestination());
            avroSchema = SchemaConverters.toAvroType(structType,
                                                     false,
                                                     "root",
                                                     SSTableTransformer.class.getCanonicalName());

            switchWriter(dataLayerWrapper.currentDestination(), options.outputFormat);
        }

        @Override
        protected void record(Object individualResult)
        {
            outputFiles.add((AbstractInputOutputFile) individualResult);
        }

        @Override
        public List<Object> getResults()
        {
            return new ArrayList<>(outputFiles);
        }

        protected long printDuration(AbstractInputOutputFile outputFile, int count, long start, long end)
        {
            logger.info("Transformed {} rows to {} in {} seconds.",
                        count,
                        outputFile.getPath(),
                        (end - start) / 1000);

            // new start
            return currentTimeMillis();
        }

        protected void switchWriter(AbstractInputOutputFile destination, OutputFormat outputFormat)
        {
            if (!destination.canWrite())
                throw new IllegalStateException("Can not write to " + destination.getPath());

            try
            {
                switch (outputFormat)
                {
                    case AVRO:
                        rowWriter = new AvroRowWriter(dataLayerWrapper.getDataLayer(),
                                                      avroSchema,
                                                      destination,
                                                      options);
                        break;
                    case PARQUET:
                        rowWriter = new ParquetRowWriter(dataLayerWrapper.getDataLayer(),
                                                         avroSchema,
                                                         destination,
                                                         options);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported output format " + outputFormat.name());
                }
            } catch (Throwable t)
            {
                throw new TransformerException("Error while closing current Spark row consumer", t);
            }
        }
    }

    private static class UnsortedFileBasedRowsWriter extends FileBasedRowsWriter
    {
        public UnsortedFileBasedRowsWriter(DataLayerWrapper dataLayerWrapper,
                                           TransformationSink transformationSink,
                                           TransformerOptions options)
        {
            super(dataLayerWrapper, transformationSink, options);
        }

        @Override
        protected void internalWrite(SparkRowIterator iterator) throws IOException
        {
            while (iterator.next())
            {
                if (count == dataLayerWrapper.getMaxRowsPerBatch())
                {
                    start = printDuration(dataLayerWrapper.currentDestination(),
                                          count,
                                          start,
                                          currentTimeMillis());

                    dataLayerWrapper.currentDestination().setCount(count);

                    close();

                    executeSink(dataLayerWrapper.currentDestination());

                    record(dataLayerWrapper.getNextDestination());
                    switchWriter(dataLayerWrapper.currentDestination(), options.outputFormat);

                    count = 0;
                }

                rowWriter.accept(iterator.get());
                count++;
            }

            if (count != 0)
                printDuration(dataLayerWrapper.currentDestination(), count, start, currentTimeMillis());
        }
    }

    private static class SortedFileBasedRowsWriter extends FileBasedRowsWriter
    {
        private final Comparator<InternalRow> rowComparator;
        private final InternalRow[] rowsBuffer;

        public SortedFileBasedRowsWriter(DataLayerWrapper dataLayerWrapper,
                                         TransformationSink transformationSink,
                                         TransformerOptions options)
        {
            super(dataLayerWrapper, transformationSink, options);
            List<DataType> javaSchema = stream(structType.fields()).map(StructField::dataType).collect(toList());
            Seq<DataType> scalaSchema = JavaConverters.asScalaBuffer(javaSchema).toSeq();
            rowComparator = RowOrdering.createNaturalAscendingOrdering(scalaSchema);

            int arraySize = (int) dataLayerWrapper.getMaxRowsPerBatch();
            if (arraySize == -1)
                arraySize = Integer.parseInt(System.getProperty("transformer.buffer.size", "10000000"));

            rowsBuffer = new InternalRow[arraySize];
        }

        @Override
        protected void internalWrite(SparkRowIterator iterator) throws IOException
        {
            boolean shouldSwitch = false;

            while (iterator.next())
            {
                if (shouldSwitch)
                {
                    close();
                    executeSink(dataLayerWrapper.currentDestination());
                    record(dataLayerWrapper.getNextDestination());
                    switchWriter(dataLayerWrapper.currentDestination(), options.outputFormat);
                    shouldSwitch = false;
                }

                rowsBuffer[count++] = iterator.get();

                if (count == dataLayerWrapper.getMaxRowsPerBatch())
                {
                    sortAndWrite(rowsBuffer);
                    start = printDuration(dataLayerWrapper.currentDestination(),
                                          count,
                                          start,
                                          currentTimeMillis());
                    dataLayerWrapper.currentDestination().setCount(count);
                    count = 0;
                    shouldSwitch = true;
                }
            }

            sortAndWrite(rowsBuffer);

            if (count != 0)
                printDuration(dataLayerWrapper.currentDestination(), count, start, currentTimeMillis());
        }

        private void sortAndWrite(InternalRow[] rows)
        {
            Arrays.sort(rows, Comparator.nullsLast(rowComparator));
            for (InternalRow sorted : rows)
            {
                if (sorted != null)
                    rowWriter.accept(sorted);
                else
                    break;
            }

            Arrays.fill(rows, null);
        }
    }
}
