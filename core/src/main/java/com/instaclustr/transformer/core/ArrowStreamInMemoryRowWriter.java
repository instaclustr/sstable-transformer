package com.instaclustr.transformer.core;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class ArrowStreamInMemoryRowWriter extends AbstractRowWriter
{
    private final ByteArrayOutputStream outputStream;
    private final InternalArrowStreamWriter writer;

    public ArrowStreamInMemoryRowWriter(StructType structType,
                                        ByteArrayOutputStream outputStream)
    {
        this.outputStream = outputStream;
        this.outputStream.reset();
        writer = new InternalArrowStreamWriter(structType, outputStream, false);
    }

    public void start() throws IOException
    {
        writer.start();
    }

    public void stop() throws IOException
    {
        writer.end();
        close();
    }

    @Override
    public void accept(InternalRow row)
    {
        writer.write(row);
    }

    @Override
    public void close()
    {
        if (writer != null)
            writer.close();
    }

    public Object getOutputStream()
    {
        return outputStream;
    }
}
