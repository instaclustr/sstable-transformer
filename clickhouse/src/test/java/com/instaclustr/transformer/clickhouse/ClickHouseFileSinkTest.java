package com.instaclustr.transformer.clickhouse;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Because of MergeTree, we have two rows when inserting two Parquet / Avro files
 * which are holding data of two respective SSTables. These SSTables were
 * processed by ONE_FILE_PER_SSTABLE strategy, so they were not compacted.
 * If ONE_FILE_ALL_SSTABLES strategy was used, then one Parquet file would be created
 * (if not limited on individual size of one Parquet file), and data in there
 * would be compacted into one row, so it would be inserted as one row into
 * Clickhouse too.
 * <p>
 * Both files in each format for test has these records:
 * - first has 1, 10, null as values
 * - second has 1, null, 20 as values
 * <p>
 * Each contains 1 row. In tests, as they can be returned in different order,
 * we make order irrelevant.
 */
public class ClickHouseFileSinkTest extends AbstractClickhouseSinkTest
{
    @Test
    public void testParquetFileImport() throws Throwable
    {
        try (ClickHouseFileSink sink = getSink(CLICKHOUSE_TABLE))
        {
            sink.sink(getFile("parquet/oa-2-big-Data.db-1.parquet"));
            sink.sink(getFile("parquet/oa-3-big-Data.db-1.parquet"));
        }

        assertContent();
    }

    @Test
    public void testAvroFileImport() throws Throwable
    {
        try (ClickHouseFileSink sink = getSink(CLICKHOUSE_TABLE))
        {
            sink.sink(getFile("avro/oa-2-big-Data.db-1.avro"));
            sink.sink(getFile("avro/oa-3-big-Data.db-1.avro"));
        }

        assertContent();
    }

    private void assertContent() throws Throwable
    {
        List<List<String>> selection = clickhouseSelect(CLICKHOUSE_TABLE);

        assertEquals(2, selection.size());
        List<String> row1 = selection.get(0);
        List<String> row2 = selection.get(1);

        assertEquals("1", row1.get(0));
        assertTrue(row1.get(1).equals("10") || row1.get(1).equals("\\N"));
        assertTrue(row1.get(2).equals("20") || row1.get(2).equals("\\N"));

        assertEquals("1", row2.get(0));
        assertTrue(row2.get(1).equals("10") || row2.get(1).equals("\\N"));
        assertTrue(row2.get(2).equals("20") || row2.get(2).equals("\\N"));
    }
}
