package com.instaclustr.transformer.api;

import java.io.Serializable;

public enum OutputFormat implements Serializable
{
    PARQUET(".parquet"),
    AVRO(".avro"),
    // extension is not in real used as data will be just in memory
    // but does not hurt to have it regardless
    ARROW_STREAM(".arrow");

    public final String fileExtension;

    OutputFormat(String fileExtension)
    {
        this.fileExtension = fileExtension;
    }

    public String getFileExtension()
    {
        return fileExtension;
    }

    public static OutputFormat fromFileName(String fileName)
    {
        if (fileName.endsWith(PARQUET.getFileExtension()))
            return PARQUET;
        else if (fileName.endsWith(AVRO.getFileExtension()))
            return AVRO;

        throw new IllegalArgumentException("unsupported file " + fileName);
    }
}
