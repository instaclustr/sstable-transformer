package com.instaclustr.transformer.core;

import com.instaclustr.transformer.api.AbstractFile;
import com.instaclustr.transformer.api.OutputFormat;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.io.SeekableInputStream;

import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * input/output naming just follows the naming convention from Parquet. Avro is also using InputFile
 * abstraction, e.g. in AvroParquetWriter's builder etc.
 * <p>
 * Integrations should work with plain AbstractFile only.
 */
public abstract class AbstractInputOutputFile extends AbstractFile implements InputFile, OutputFile
{
    private RandomAccessFile file;

    public AbstractInputOutputFile(OutputFormat outputFormat, Path path, int number)
    {
        super(outputFormat, path, number);
    }

    /**
     * Returns logically next output file a transformation process should write data to when this one is considered full.
     *
     * @return next output file.
     */
    @Override
    public abstract AbstractInputOutputFile next();

    @Override
    public String toString()
    {
        return getPath();
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException
    {
        return new PositionOutputStreamWrapper(Files.newOutputStream(Paths.get(getPath())));
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException
    {
        return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize()
    {
        return false;
    }

    @Override
    public long defaultBlockSize()
    {
        return 0;
    }

    @Override
    public long getLength() throws IOException
    {
        return Files.size(Paths.get(getPath()));
    }

    @Override
    public SeekableInputStream newStream() throws IOException
    {
        if (file == null)
        {
            file = new RandomAccessFile(getPath(), "r");
        }

        return new DelegatingSeekableInputStream(Channels.newInputStream(file.getChannel()))
        {
            @Override
            public long getPos() throws IOException
            {
                return file.getFilePointer();
            }

            @Override
            public void seek(long newPos) throws IOException
            {
                file.seek(newPos);
            }
        };
    }

    private static class PositionOutputStreamWrapper extends PositionOutputStream
    {
        private final OutputStream out;
        private long position = 0;

        PositionOutputStreamWrapper(OutputStream out)
        {
            this.out = out;
        }

        @Override
        public long getPos()
        {
            return position;
        }

        @Override
        public void write(int b) throws IOException
        {
            out.write(b);
            position++;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException
        {
            out.write(b, off, len);
            position += len;
        }

        @Override
        public void flush() throws IOException
        {
            out.flush();
        }

        @Override
        public void close() throws IOException
        {
            out.close();
        }
    }
}
