package com.instaclustr.transformer.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.data.ClickHouseFormat;
import com.instaclustr.transformer.api.AbstractFile;
import com.instaclustr.transformer.api.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Name of this sink is {@code clickhouse-file}.
 * <p>
 * Takes {@link AbstractFile} which is backed, presumably, by Parquet / Avro file,
 * and it will insert it into a Clickhouse table via client's
 * {@link Client#insert(String, InputStream, ClickHouseFormat)}.
 * <p>
 * This sink does not check if Parquet / Avro types correspond to correct types
 * in Clickhouse table. If the columns do not match, then sinking will fail and
 * caller needs to deal with it.
 * <p>
 * Only HTTP protocol is possible to use for now. Clickhouse Java library does not
 * support other protocols anyway at the time of coding this solution.
 * <p>
 */
public class ClickHouseFileSink extends AbstractClickHouseSink
{
    private static final Logger logger = LoggerFactory.getLogger(ClickHouseFileSink.class);

    public ClickHouseFileSink()
    {
        super(null);
        // for spi
    }

    public ClickHouseFileSink(ClickHouseConfig config)
    {
        super(config);
    }

    @Override
    public synchronized void sink(Object sinkObject) throws Exception
    {
        if (!(sinkObject instanceof AbstractFile))
        {
            throw new IllegalArgumentException("sink object is not an instance of " + AbstractFile.class.getName());
        }

        AbstractFile fileToSink = (AbstractFile) sinkObject;
        OutputFormat outputFormat = fileToSink.getOutputFormat();

        if (!supports(outputFormat))
            throw new IllegalStateException("this sink does not support " + outputFormat + " output format.");

        try (FileInputStream fis = new FileInputStream(fileToSink.getPath()))
        {
            client.insert(config.table, fis, getFormat(outputFormat));
        } finally
        {
            maybeRemoveFile(fileToSink);
        }
    }

    private void maybeRemoveFile(AbstractFile sinkObject)
    {
        if (config.removeFileAfterProcessed)
        {
            try
            {
                Files.delete(Paths.get(sinkObject.getPath()));
            } catch (Throwable t)
            {
                logger.warn(String.format("Unable to delete processed file: %s, reason: %s",
                                          sinkObject.getPath(),
                                          t.getMessage()));
            }
        }
    }

    @Override
    public Class<?> inputObjectType()
    {
        return AbstractFile.class;
    }

    @Override
    public boolean supports(OutputFormat format)
    {
        return format == OutputFormat.PARQUET || format == OutputFormat.AVRO;
    }

    private ClickHouseFormat getFormat(OutputFormat outputFormat)
    {
        if (outputFormat == OutputFormat.PARQUET)
            return ClickHouseFormat.Parquet;
        else if (outputFormat == OutputFormat.AVRO)
            return ClickHouseFormat.Avro;

        throw new UnsupportedOperationException("Invalid format " + outputFormat);
    }
}
