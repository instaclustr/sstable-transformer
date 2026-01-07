package com.instaclustr.transformer.clickhouse;

import com.instaclustr.transformer.api.OutputFormat;
import com.instaclustr.transformer.core.SSTableTransformer;
import com.instaclustr.transformer.core.TransformerOptions;
import com.instaclustr.transformer.core.TransformerOptions.TransformationStrategy;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the file-based approach: SSTable -> Parquet/Avro files -> ClickHouse via ClickHouseFileSink.
 * <p>
 * Parameterized across two axes:
 * <ul>
 *     <li>Output format: PARQUET, AVRO</li>
 *     <li>Strategy: ONE_FILE_ALL_SSTABLES, ONE_FILE_PER_SSTABLE</li>
 * </ul>
 */
public class ClickHouseFileImportTest extends AbstractClickhouseSinkTest
{
    static Stream<Arguments> formatAndStrategy()
    {
        return Stream.of(
                Arguments.of(OutputFormat.PARQUET, TransformationStrategy.ONE_FILE_ALL_SSTABLES),
                Arguments.of(OutputFormat.PARQUET, TransformationStrategy.ONE_FILE_PER_SSTABLE),
                Arguments.of(OutputFormat.AVRO, TransformationStrategy.ONE_FILE_ALL_SSTABLES),
                Arguments.of(OutputFormat.AVRO, TransformationStrategy.ONE_FILE_PER_SSTABLE));
    }

    @ParameterizedTest(name = "{0} / {1}")
    @MethodSource("formatAndStrategy")
    public void testFileImport(OutputFormat format, TransformationStrategy strategy, @TempDir Path tempDir) throws Throwable
    {
        TransformerOptions options = new TransformerOptions();
        options.createTableStmt = "CREATE TABLE spark_test.testtable (id int primary key)";
        options.transformationStrategy = strategy;
        options.outputFormat = format;
        options.input = List.of(Paths.get("src/test/resources/sstables").toAbsolutePath().toString());
        options.output = tempDir.toAbsolutePath().toString();
        options.sinkConfig = Paths.get("src/test/resources/clickhouse-sink-file.properties").toAbsolutePath();

        options.validate();

        new SSTableTransformer(options).runTransformation(ClickHouseFileSink.class);

        assertEquals(10_000, clickhouseSelect(CLICKHOUSE_TABLE_SIMPLE).size());
    }
}
