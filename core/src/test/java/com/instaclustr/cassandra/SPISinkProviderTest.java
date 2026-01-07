package com.instaclustr.cassandra;

import com.instaclustr.transformer.api.OutputFormat;
import com.instaclustr.transformer.api.TransformationSink;
import com.instaclustr.transformer.core.SPISinkProvider;
import com.instaclustr.transformer.core.TransformationSinkProvider;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.util.ServiceLoader;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.doReturn;

public class SPISinkProviderTest
{
    public static class MySink implements TransformationSink
    {
        @Override
        public void close()
        {

        }

        @Override
        public void init(Path sinkConfigFile)
        {

        }

        @Override
        public void sink(Object sinkObject)
        {

        }

        @Override
        public Class<?> inputObjectType()
        {
            return ByteArrayOutputStream.class;
        }

        @Override
        public boolean supports(OutputFormat format)
        {
            return true;
        }
    }

    @Test
    public void testSinkIsLoaded()
    {
        ServiceLoader<TransformationSink> loader = Mockito.mock(ServiceLoader.class);
        doReturn(Stream.of(new TransformationSinkProvider(MySink.class))).when(loader).stream();

        try (MockedStatic<ServiceLoader> serviceLoader = Mockito.mockStatic(ServiceLoader.class))
        {
            serviceLoader.when(() -> ServiceLoader.load(TransformationSink.class)).thenReturn(loader);
            ServiceLoader.Provider<TransformationSink> sinkProvider = SPISinkProvider.getSinkProvider(SPISinkProviderTest.MySink.class.getName());
            TransformationSink transformationSink = sinkProvider.get();
            assertEquals(MySink.class, transformationSink.getClass());
        }
    }

    @Test
    public void testNoSinkIsIllegal()
    {
        ServiceLoader<TransformationSink> loader = Mockito.mock(ServiceLoader.class);
        doReturn(Stream.of(new TransformationSinkProvider(MySink.class))).when(loader).stream();

        try (MockedStatic<ServiceLoader> serviceLoader = Mockito.mockStatic(ServiceLoader.class))
        {
            serviceLoader.when(() -> ServiceLoader.load(TransformationSink.class)).thenReturn(loader);

            try
            {
                SPISinkProvider.getSinkProvider("no-such-sink");
                fail("should fail on non-existent sink implementation");
            } catch (Throwable t)
            {
                assertEquals("There are no sink provider of name no-such-sink.", t.getMessage());
            }
        }
    }

    @Test
    public void testNotUniqueNameIsIllegal()
    {
        ServiceLoader<TransformationSink> loader = Mockito.mock(ServiceLoader.class);

        /// twice!!!

        doReturn(Stream.of(new TransformationSinkProvider(MySink.class),
                           new TransformationSinkProvider(MySink.class))).when(loader).stream();

        try (MockedStatic<ServiceLoader> serviceLoader = Mockito.mockStatic(ServiceLoader.class))
        {
            serviceLoader.when(() -> ServiceLoader.load(TransformationSink.class)).thenReturn(loader);

            try
            {
                SPISinkProvider.getSinkProvider(SPISinkProviderTest.MySink.class.getName());
                fail("should fail on non-unique implementation");
            } catch (Throwable t)
            {
                assertEquals(String.format("Not possible to have more than 1 implementation of sink of name %s but we have: [%s, %s]",
                                           SPISinkProviderTest.MySink.class.getName(),
                                           SPISinkProviderTest.MySink.class.getName(),
                                           SPISinkProviderTest.MySink.class.getName()),
                             t.getMessage());
            }
        }
    }
}
