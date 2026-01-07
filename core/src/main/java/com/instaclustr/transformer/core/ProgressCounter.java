package com.instaclustr.transformer.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public final class ProgressCounter
{
    private static final Logger logger = LoggerFactory.getLogger(ProgressCounter.class);
    private static final AtomicLong counter = new AtomicLong(0);

    public static void add(long count)
    {
        counter.addAndGet(count);
    }

    public static long get()
    {
        return counter.get();
    }

    public static void log()
    {
        logger.info("Processed rows: " + counter.get());
    }
}
