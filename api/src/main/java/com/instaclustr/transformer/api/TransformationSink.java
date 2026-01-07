package com.instaclustr.transformer.api;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;

import static com.instaclustr.transformer.api.OutputFormat.ARROW_STREAM;
import static com.instaclustr.transformer.api.OutputFormat.AVRO;
import static com.instaclustr.transformer.api.OutputFormat.PARQUET;
import static java.lang.String.format;

/**
 * Implementations might react on a creation of a Parquet / Avro file
 * and transform it further.
 */
public interface TransformationSink extends AutoCloseable
{
    /**
     * Method called first on this sink, chance to set it up.
     * <p>
     * The format of a configuration file for a sink is sink's responsibility to manage.
     *
     * @param sinkConfigFile generic file with a configuration.
     */
    void init(Path sinkConfigFile) throws Exception;

    /**
     * @param sinkObject object to sink to this transformer
     */
    void sink(Object sinkObject) throws Exception;

    /**
     * @return type of input object of {@link TransformationSink#sink}.
     */
    Class<?> inputObjectType();

    /**
     * Checks if this sink supports given output format.
     *
     * @param format format to check
     * @return true if this sink supports processing this outpuf format of the primary transformation
     */
    boolean supports(OutputFormat format);

    /**
     * If sink is null, it throws when output format is {@link OutputFormat#ARROW_STREAM}.
     * <p>
     * Throws when {@code outputFormat} is not among supported ones via {@link TransformationSink#supports(OutputFormat)}.
     * <p>
     * Throws when {@link TransformationSink#inputObjectType()} is not equal {@link AbstractFile}
     * for output formats of {@link OutputFormat#AVRO} or {@link OutputFormat#PARQUET}.
     *
     * @param transformationSink sink to use
     * @param outputFormat       format of transformation
     */
    static void validate(TransformationSink transformationSink, OutputFormat outputFormat)
    {
        if (transformationSink == null)
        {
            if (outputFormat == ARROW_STREAM)
            {
                throw new IllegalStateException("You have to use some sink when having output format of " + ARROW_STREAM);
            }

            return;
        }

        if (!transformationSink.supports(outputFormat))
        {
            throw new IllegalStateException(format("Sink '%s' does not support output format %s",
                                                   transformationSink.getClass().getName(),
                                                   outputFormat));
        }

        if (outputFormat == AVRO || outputFormat == PARQUET)
        {
            if (transformationSink.inputObjectType() != AbstractFile.class)
            {
                throw new IllegalStateException(format("Sink '%s' can not accept anything but %s",
                                                       transformationSink.getClass().getName(),
                                                       transformationSink.inputObjectType().getName()));
            }
        }
        else if (outputFormat == ARROW_STREAM)
        {
            if (transformationSink.inputObjectType() != ByteArrayOutputStream.class)
            {
                throw new IllegalStateException(format("Sink '%s' can not accept anything but %s",
                                                       transformationSink.getClass().getName(),
                                                       transformationSink.inputObjectType().getName()));
            }
        }
        else
        {
            throw new IllegalStateException(format("Sink %s is not supporting output format %s",
                                                   transformationSink.getClass().getName(),
                                                   outputFormat));
        }
    }
}
