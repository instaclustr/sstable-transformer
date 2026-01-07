package com.instaclustr.transformer.core;

import com.instaclustr.transformer.api.TransformationSink;

import java.util.ServiceLoader;

public class TransformationSinkProvider implements ServiceLoader.Provider<TransformationSink>
{
    private final Class<? extends TransformationSink> sinkClass;

    public TransformationSinkProvider(Class<? extends TransformationSink> sinkClass)
    {
        this.sinkClass = sinkClass;
    }

    @Override
    public Class<? extends TransformationSink> type()
    {
        return sinkClass;
    }

    @Override
    public TransformationSink get()
    {
        try
        {
            return type().getDeclaredConstructor().newInstance();
        }
        catch (Throwable t)
        {
            throw new IllegalStateException("Unable to create an instance of " + type().getName(), t);
        }
    }
}
