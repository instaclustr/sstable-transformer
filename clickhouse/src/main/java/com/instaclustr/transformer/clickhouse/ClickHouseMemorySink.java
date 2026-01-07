package com.instaclustr.transformer.clickhouse;

import com.clickhouse.data.ClickHouseFormat;
import com.instaclustr.transformer.api.ExposedByteArrayOutputStream;
import com.instaclustr.transformer.api.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class ClickHouseMemorySink extends AbstractClickHouseSink
{
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseMemorySink.class);

    public ClickHouseMemorySink()
    {
        super(null);
        // for spi
    }

    public ClickHouseMemorySink(ClickHouseConfig config)
    {
        super(config);
    }

    @Override
    public void sink(Object sinkObject) throws Exception
    {
        if (!(sinkObject instanceof ExposedByteArrayOutputStream))
            throw new IllegalArgumentException("sink object is not an instance of " + ByteArrayOutputStream.class.getName());

        ExposedByteArrayOutputStream outputStream = (ExposedByteArrayOutputStream) sinkObject;
        long start = System.currentTimeMillis();

        try (InputStream is = new ByteArrayInputStream(outputStream.getBuffer(), 0, outputStream.size()))
        {
            client.insert(config.table, is, ClickHouseFormat.ArrowStream);
        }
        long stop = System.currentTimeMillis();

        logger.info("Insert took " + (stop - start));
    }

    @Override
    public Class<?> inputObjectType()
    {
        return ByteArrayOutputStream.class;
    }

    @Override
    public boolean supports(OutputFormat format)
    {
        return format == OutputFormat.ARROW_STREAM;
    }
}
