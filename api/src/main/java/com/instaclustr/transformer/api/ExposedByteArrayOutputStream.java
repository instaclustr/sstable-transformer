package com.instaclustr.transformer.api;

import java.io.ByteArrayOutputStream;

/**
 * A {@link ByteArrayOutputStream} that exposes its internal buffer
 * to avoid the copy performed by {@link ByteArrayOutputStream#toByteArray()}.
 */
public class ExposedByteArrayOutputStream extends ByteArrayOutputStream
{
    public ExposedByteArrayOutputStream(int size)
    {
        super(size);
    }

    /**
     * Returns the internal buffer directly, without copying.
     * Only bytes from index 0 to {@link #size()} are valid.
     */
    public byte[] getBuffer()
    {
        return buf;
    }
}
