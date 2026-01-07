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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

public abstract class AbstractRowWriter implements Consumer<InternalRow>, AutoCloseable
{
    public static GenericRecord convertInternalRowToAvro(InternalRow row, StructType schema, Schema avroSchema)
    {
        GenericRecord record = new GenericData.Record(avroSchema);

        StructField[] fields = schema.fields();
        for (int i = 0; i < fields.length; i++)
        {
            String name = fields[i].name();
            DataType type = fields[i].dataType();
            Object value;

            if (row.isNullAt(i))
            {
                value = null;
            } else if (type == DataTypes.BooleanType)
            {
                value = row.getBoolean(i);
            } else if (type == DataTypes.StringType)
            {
                value = row.getUTF8String(i).toString();
            } else if (type == DataTypes.ShortType)
            {
                value = row.getShort(i);
            } else if (type == DataTypes.IntegerType)
            {
                value = row.getInt(i);
            } else if (type == DataTypes.LongType)
            {
                value = row.getLong(i);
            } else if (type == DataTypes.DoubleType)
            {
                value = row.getDouble(i);
            } else if (type == DataTypes.FloatType)
            {
                value = row.getFloat(i);
            } else if (type == DataTypes.BinaryType)
            {
                value = ByteBuffer.wrap(row.getBinary(i));
            } else if (type == DataTypes.TimestampType)
            {
                value = row.getLong(i);
            } else if (type == DataTypes.DateType)
            {
                value = row.getInt(i);
            } else
            {
                throw new UnsupportedOperationException("Unsupported data type: " + type);
            }

            record.put(name, value);
        }

        return record;
    }
}
