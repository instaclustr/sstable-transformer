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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;

/**
 * Writes {@link InternalRow} into a Parquet file.
 */
public class ParquetRowWriter extends AbstractRowWriter
{
    private final ParquetWriter<GenericRecord> writer;
    private final StructType structType;
    private final Schema schema;
    private final AbstractInputOutputFile destination;

    /**
     * @param dataLayer   Data layer to use
     * @param schema      Avro schema used for {@link ParquetWriter}.
     * @param destination Initial destination to write data to.
     * @param options     Transformation options
     * @throws IOException if anything IO-related goes wrong
     */
    public ParquetRowWriter(DataLayer dataLayer,
                            Schema schema,
                            AbstractInputOutputFile destination,
                            TransformerOptions options) throws IOException
    {
        this.structType = dataLayer.structType();
        this.schema = schema;
        this.destination = destination;
        writer = createWriter(this.destination,
                              options.bloomFilterEnabled,
                              options.compression);
    }

    @Override
    public void accept(InternalRow row)
    {
        try
        {
            writer.write(convertInternalRowToAvro(row, structType, schema));
        }
        catch (Throwable t)
        {
            throw new TransformerException("Unable to write row", t);
        }
    }

    private ParquetWriter<GenericRecord> createWriter(AbstractInputOutputFile destination,
                                                      boolean bloomFilterEnabled,
                                                      TransformerOptions.Compression compression) throws IOException
    {
        return AvroParquetWriter.<GenericRecord>builder(destination)
                .enablePageWriteChecksum()
                .enableValidation()
                .withBloomFilterEnabled(bloomFilterEnabled)
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(compression.forParquet())
                .withSchema(schema)
                .build();
    }

    @Override
    public void close() throws Exception
    {
        destination.finish();
        writer.close();
    }
}
