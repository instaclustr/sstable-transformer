package com.instaclustr.transformer.clickhouse;

import com.instaclustr.transformer.api.OutputFormat;
import com.instaclustr.transformer.core.SSTableTransformer;
import com.instaclustr.transformer.core.TransformerOptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ClickHouseMemorySinkTest extends AbstractClickhouseSinkTest
{
    @Test
    public void testArrowStreamImportByteBufferSinkMode() throws Throwable
    {
        TransformerOptions options = getOptions("byte-buffer");
        new SSTableTransformer(options).runTransformation(ClickHouseMemorySink.class);
        assertEquals(10_000, clickhouseSelect(CLICKHOUSE_TABLE_SIMPLE).size());
    }

    @Test
    public void testArrowStreamImportPipeSinkMode() throws Throwable
    {
        TransformerOptions options = getOptions("pipe");
        new SSTableTransformer(options).runTransformation(ClickHouseMemorySink.class);
        assertEquals(10_000, clickhouseSelect(CLICKHOUSE_TABLE_SIMPLE).size());
    }

    private TransformerOptions getOptions(String sinkMode)
    {
        TransformerOptions options = new TransformerOptions();
        options.createTableStmt = "CREATE TABLE spark_test.testtable (id int primary key)";
        options.transformationStrategy = TransformerOptions.TransformationStrategy.ONE_FILE_ALL_SSTABLES;
        options.outputFormat = OutputFormat.ARROW_STREAM;

        options.input = List.of(Paths.get("src/test/resources/sstables").toAbsolutePath().toString());
        options.sinkConfig = Paths.get("src/test/resources/clickhouse-sink-" + sinkMode + ".properties").toAbsolutePath();

        options.validate();

        return options;
    }
}
