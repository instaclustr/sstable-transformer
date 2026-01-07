package com.instaclustr.transformer.core;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.BinaryType;
import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.ByteType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.DoubleType;
import org.apache.spark.sql.types.FloatType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;
import org.apache.spark.unsafe.types.UTF8String;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;

import static org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE;
import static org.apache.arrow.vector.types.FloatingPointPrecision.SINGLE;

class InternalArrowStreamWriter implements AutoCloseable
{
    private static final Logger logger = LoggerFactory.getLogger(InternalArrowStreamWriter.class);

    static final int DEFAULT_BATCH_SIZE = 10000;

    private final RootAllocator allocator;
    private final VectorSchemaRoot root;
    private final ArrowStreamWriter writer;
    private final StructType structType;
    private final OutputStream outputStream;
    private final boolean flushBeforeWrite;
    private final int batchSize;
    private int currentRowIndex;

    InternalArrowStreamWriter(StructType structType,
                              OutputStream outputStream,
                              boolean flushBeforeWrite)
    {
        this(structType, outputStream, flushBeforeWrite, DEFAULT_BATCH_SIZE);
    }

    InternalArrowStreamWriter(StructType structType,
                              OutputStream outputStream,
                              boolean flushBeforeWrite,
                              int batchSize)
    {
        this.structType = structType;
        this.outputStream = outputStream;
        this.flushBeforeWrite = flushBeforeWrite;
        this.batchSize = batchSize;
        Schema schema = buildSchema(structType);
        allocator = new RootAllocator();
        root = VectorSchemaRoot.create(schema, allocator);
        writer = new ArrowStreamWriter(root, null, Channels.newChannel(outputStream));
        currentRowIndex = 0;
    }

    public void start() throws IOException
    {
        writer.start();
    }

    public void end() throws IOException
    {
        if (flushBeforeWrite)
            outputStream.flush();

        if (currentRowIndex > 0)
        {
            root.setRowCount(currentRowIndex);
            writer.writeBatch();
            currentRowIndex = 0;
        }
        writer.end();

        if (!flushBeforeWrite)
            outputStream.flush();
    }

    @Override
    public void close()
    {
        try
        {
            writer.close();
            root.close();
            allocator.close();
        }
        catch (Throwable t)
        {
            throw new TransformerException("Unable to fully close " + InternalArrowStreamWriter.class.getName(), t);
        }
    }

    public void write(InternalRow row)
    {
        try
        {
            StructField[] fields = structType.fields();

            for (int i = 0; i < fields.length; i++)
            {
                StructField field = fields[i];
                FieldVector vector = root.getVector(field.name());
                writeValue(vector, row, i, field.dataType());
            }

            currentRowIndex++;

            if (currentRowIndex >= batchSize)
            {
                root.setRowCount(currentRowIndex);
                writer.writeBatch();
                outputStream.flush();
                currentRowIndex = 0;
                root.allocateNew();
            }
        }
        catch (Exception e)
        {
            throw new TransformerException("Failed to write row to Arrow stream", e);
        }
    }

    static Schema buildSchema(StructType structType)
    {
        List<Field> fields = new ArrayList<>();

        for (StructField sparkField : structType.fields())
        {
            String fieldName = sparkField.name();
            DataType sparkType = sparkField.dataType();
            boolean nullable = sparkField.nullable();

            ArrowType arrowType = sparkTypeToArrowType(sparkType);

            if (arrowType != null)
            {
                FieldType fieldType = nullable ?
                        FieldType.nullable(arrowType) :
                        new FieldType(false, arrowType, null, null);

                fields.add(new Field(fieldName, fieldType, null));
            } else
            {
                logger.warn("Unsupported Spark type: {} for field: {}, skipping", sparkType, fieldName);
            }
        }

        return new Schema(fields);
    }

    private static ArrowType sparkTypeToArrowType(DataType sparkType)
    {
        if (sparkType instanceof IntegerType)
        {
            return new ArrowType.Int(32, true);
        } else if (sparkType instanceof LongType)
        {
            return new ArrowType.Int(64, true);
        } else if (sparkType instanceof ShortType)
        {
            return new ArrowType.Int(16, true);
        } else if (sparkType instanceof ByteType)
        {
            return new ArrowType.Int(8, true);
        } else if (sparkType instanceof FloatType)
        {
            return new ArrowType.FloatingPoint(SINGLE);
        } else if (sparkType instanceof DoubleType)
        {
            return new ArrowType.FloatingPoint(DOUBLE);
        } else if (sparkType instanceof StringType)
        {
            return new ArrowType.Utf8();
        } else if (sparkType instanceof BooleanType)
        {
            return new ArrowType.Bool();
        } else if (sparkType instanceof DateType)
        {
            return new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY);
        } else if (sparkType instanceof TimestampType)
        {
            return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MICROSECOND, "UTC");
        } else if (sparkType instanceof BinaryType)
        {
            return new ArrowType.Binary();
        } else if (sparkType instanceof DecimalType)
        {
            DecimalType decimalType = (DecimalType) sparkType;
            return new ArrowType.Decimal(decimalType.precision(), decimalType.scale());
        }

        return null;
    }

    private void writeValue(FieldVector vector, InternalRow row, int columnIndex, DataType sparkType)
    {
        if (sparkType instanceof IntegerType)
        {
            IntVector intVector = (IntVector) vector;
            if (row.isNullAt(columnIndex))
            {
                intVector.setNull(currentRowIndex);
            } else
            {
                intVector.setSafe(currentRowIndex, row.getInt(columnIndex));
            }
        } else if (sparkType instanceof LongType)
        {
            BigIntVector longVector = (BigIntVector) vector;
            if (row.isNullAt(columnIndex))
            {
                longVector.setNull(currentRowIndex);
            } else
            {
                longVector.setSafe(currentRowIndex, row.getLong(columnIndex));
            }
        } else if (sparkType instanceof ShortType)
        {
            SmallIntVector shortVector = (SmallIntVector) vector;
            if (row.isNullAt(columnIndex))
            {
                shortVector.setNull(currentRowIndex);
            } else
            {
                shortVector.setSafe(currentRowIndex, row.getShort(columnIndex));
            }
        } else if (sparkType instanceof ByteType)
        {
            TinyIntVector byteVector = (TinyIntVector) vector;
            if (row.isNullAt(columnIndex))
            {
                byteVector.setNull(currentRowIndex);
            } else
            {
                byteVector.setSafe(currentRowIndex, row.getByte(columnIndex));
            }
        } else if (sparkType instanceof FloatType)
        {
            Float4Vector floatVector = (Float4Vector) vector;
            if (row.isNullAt(columnIndex))
            {
                floatVector.setNull(currentRowIndex);
            } else
            {
                floatVector.setSafe(currentRowIndex, row.getFloat(columnIndex));
            }
        } else if (sparkType instanceof DoubleType)
        {
            Float8Vector doubleVector = (Float8Vector) vector;
            if (row.isNullAt(columnIndex))
            {
                doubleVector.setNull(currentRowIndex);
            } else
            {
                doubleVector.setSafe(currentRowIndex, row.getDouble(columnIndex));
            }
        } else if (sparkType instanceof StringType)
        {
            VarCharVector varCharVector = (VarCharVector) vector;
            if (row.isNullAt(columnIndex))
            {
                varCharVector.setNull(currentRowIndex);
            } else
            {
                UTF8String utf8String = row.getUTF8String(columnIndex);
                byte[] bytes = utf8String.getBytes();
                varCharVector.setSafe(currentRowIndex, bytes);
            }
        } else if (sparkType instanceof BooleanType)
        {
            BitVector bitVector = (BitVector) vector;
            if (row.isNullAt(columnIndex))
            {
                bitVector.setNull(currentRowIndex);
            } else
            {
                bitVector.setSafe(currentRowIndex, row.getBoolean(columnIndex) ? 1 : 0);
            }
        } else if (sparkType instanceof DateType)
        {
            DateDayVector dateVector = (DateDayVector) vector;
            if (row.isNullAt(columnIndex))
            {
                dateVector.setNull(currentRowIndex);
            } else
            {
                dateVector.setSafe(currentRowIndex, row.getInt(columnIndex));
            }
        } else if (sparkType instanceof TimestampType)
        {
            TimeStampMicroVector timestampVector = (TimeStampMicroVector) vector;
            if (row.isNullAt(columnIndex))
            {
                timestampVector.setNull(currentRowIndex);
            } else
            {
                timestampVector.setSafe(currentRowIndex, row.getLong(columnIndex));
            }
        } else if (sparkType instanceof BinaryType)
        {
            VarBinaryVector binaryVector = (VarBinaryVector) vector;
            if (row.isNullAt(columnIndex))
            {
                binaryVector.setNull(currentRowIndex);
            } else
            {
                byte[] bytes = row.getBinary(columnIndex);
                binaryVector.setSafe(currentRowIndex, bytes);
            }
        } else if (sparkType instanceof DecimalType)
        {
            DecimalVector decimalVector = (DecimalVector) vector;
            if (row.isNullAt(columnIndex))
            {
                decimalVector.setNull(currentRowIndex);
            } else
            {
                DecimalType decimalType = (DecimalType) sparkType;
                org.apache.spark.sql.types.Decimal decimal = row.getDecimal(columnIndex,
                                                                            decimalType.precision(), decimalType.scale());
                decimalVector.setSafe(currentRowIndex, decimal.toBigDecimal().bigDecimal());
            }
        } else
        {
            throw new UnsupportedOperationException("Unsupported Spark type: " + sparkType);
        }
    }
}
