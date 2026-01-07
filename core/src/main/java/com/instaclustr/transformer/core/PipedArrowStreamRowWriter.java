package com.instaclustr.transformer.core;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.PipedOutputStream;

public class PipedArrowStreamRowWriter extends AbstractRowWriter
{
    private static final Logger logger = LoggerFactory.getLogger(PipedArrowStreamRowWriter.class);

    private final PipedOutputStream outputStream;
    private final InternalArrowStreamWriter writer;

    public PipedArrowStreamRowWriter(StructType structType,
                                     PipedOutputStream outputStream)
    {
        this.outputStream = outputStream;
        writer = new InternalArrowStreamWriter(structType, outputStream, true);
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
        {
            writer.close();
        }

        if (outputStream != null)
        {
            try
            {
                outputStream.close();
            }
            catch (Throwable t)
            {
                logger.error("Not possible to close output stream", t);
            }
        }
    }
}
