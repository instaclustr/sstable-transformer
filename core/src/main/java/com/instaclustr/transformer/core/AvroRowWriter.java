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

import com.instaclustr.transformer.api.AbstractFile;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.cassandra.spark.data.DataLayer;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;

/**
 * Writes {@link InternalRow} into an Avro file.
 */
public class AvroRowWriter extends AbstractRowWriter
{
    private final DataFileWriter<GenericRecord> writer;
    private final StructType structType;
    private final Schema schema;
    private final AbstractFile destination;

    /**
     * @param dataLayer   Data layer to use
     * @param schema      Avro schema used for {@link ParquetWriter}.
     * @param destination Initial destination to write data to.
     * @param options     Transformer options to use
     * @throws IOException if anything IO-related goes wrong
     */
    public AvroRowWriter(DataLayer dataLayer,
                         Schema schema,
                         AbstractFile destination,
                         TransformerOptions options) throws IOException
    {
        this.structType = dataLayer.structType();
        this.destination = destination;
        this.schema = schema;
        writer = createWriter(options);
    }

    @Override
    public void accept(InternalRow row)
    {
        try
        {
            writer.append(convertInternalRowToAvro(row, structType, schema));
        }
        catch (Throwable t)
        {
            throw new TransformerException("Unable to write row", t);
        }
    }

    private DataFileWriter<GenericRecord> createWriter(TransformerOptions options) throws IOException
    {
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> writer = new DataFileWriter<>(datumWriter);
        writer.setCodec(options.compression.forAvro());
        writer.create(schema, new File(destination.getPath()));
        return writer;
    }

    @Override
    public void close() throws Exception
    {
        destination.finish();
        writer.close();
    }
}