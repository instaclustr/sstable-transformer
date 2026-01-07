package com.instaclustr.transformer.clickhouse;

import com.instaclustr.transformer.api.OutputFormat;
import com.instaclustr.transformer.core.SSTableTransformer;
import com.instaclustr.transformer.core.TransformerOptions;
import com.instaclustr.transformer.core.TransformerOptions.TransformationStrategy;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClickHouseMemorySinkTest extends AbstractClickhouseSinkTest
{
    @ParameterizedTest(name = "{0}")
    @EnumSource(TransformationStrategy.class)
    public void testArrowStreamImport(TransformationStrategy strategy) throws Throwable
    {
        TransformerOptions options = new TransformerOptions();
        options.createTableStmt = "CREATE TABLE spark_test.testtable (id int primary key)";
        options.transformationStrategy = strategy;
        options.outputFormat = OutputFormat.ARROW_STREAM;
        options.input = List.of(Paths.get("src/test/resources/sstables").toAbsolutePath().toString());
        options.sinkConfig = Paths.get("src/test/resources/clickhouse-sink-async-byte-buffer.properties").toAbsolutePath();

        options.validate();

        new SSTableTransformer(options).runTransformation(ClickHouseMemorySink.class);
        assertEquals(10_000, clickhouseSelect(CLICKHOUSE_TABLE_SIMPLE).size());
    }
}
