package com.instaclustr.transformer.core;

import com.instaclustr.transformer.api.TransformationSink;

import java.util.List;
import java.util.ServiceLoader;
import java.util.ServiceLoader.Provider;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class SPISinkProvider
{
    public static Provider<TransformationSink> getSinkProvider(String sinkName)
    {
        if (sinkName == null)
            return null;

        List<Provider<TransformationSink>> allProviders = ServiceLoader.load(TransformationSink.class).stream().collect(toList());
        List<Provider<TransformationSink>> providersOfGivenName = allProviders.stream().filter(p -> sinkName.equals(p.type().getName())).collect(toList());

        if (providersOfGivenName.isEmpty())
        {
            throw new IllegalStateException(format("There is no sink provider of name %s.", sinkName));
        }

        if (providersOfGivenName.size() > 1)
        {
            throw new IllegalStateException(format("Not possible to have more than 1 implementation of sink of name %s but we have: %s",
                                                   sinkName,
                                                   providersOfGivenName.stream()
                                                           .map(p -> p.type().getName())
                                                           .collect(toList())));
        }

        return providersOfGivenName.get(0);
    }
}
