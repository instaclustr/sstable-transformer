package com.instaclustr.transformer.clickhouse;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import com.instaclustr.transformer.api.AbstractFile;
import com.instaclustr.transformer.api.OutputFormat;
import com.instaclustr.transformer.api.SinkModel;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.clickhouse.ClickHouseContainer;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractClickhouseSinkTest
{
    private static final String CLICKHOUSE_IMAGE = "clickhouse/clickhouse-server:latest";
    private static final String CLICKHOUSE_USERNAME = "clickhouse";
    private static final String CLICKHOUSE_PASSWORD = "clickhouse";
    private static final String CLICKHOUSE_INIT_SCRIPT = "init.sql";
    private static final String CLICKHOUSE_DB = "clickhouse_db";
    public static final String CLICKHOUSE_TABLE = "my_clickhouse_table";
    public static final String CLICKHOUSE_TABLE_SIMPLE = "my_clickhouse_table_simple";

    public static ClickHouseContainer clickhouse;

    @BeforeAll
    public static void beforeAll()
    {
        clickhouse = new ClickHouseContainer(CLICKHOUSE_IMAGE)
                .withInitScript(CLICKHOUSE_INIT_SCRIPT)
                .withPassword(CLICKHOUSE_PASSWORD)
                .withUsername(CLICKHOUSE_USERNAME)
                .withDatabaseName(CLICKHOUSE_DB)
                .withEnv("CLICKHOUSE_DB", CLICKHOUSE_DB);

        clickhouse.setPortBindings(List.of("8123:8123", "9000:9000"));

        clickhouse.start();
    }

    @AfterAll
    public static void afterAll()
    {
        clickhouse.stop();
    }

    @BeforeEach
    public void beforeEach()
    {
        try (Client client = getClient(CLICKHOUSE_TABLE))
        {
            client.execute("TRUNCATE TABLE " + CLICKHOUSE_TABLE).get();
        } catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
        try (Client client = getClient(CLICKHOUSE_TABLE_SIMPLE))
        {
            client.execute("TRUNCATE TABLE " + CLICKHOUSE_TABLE_SIMPLE).get();
        } catch (Throwable t)
        {
            throw new RuntimeException(t);
        }
    }

    protected ClickHouseFileSink getSink(String clickhouseTable)
    {
        return new ClickHouseFileSink(getConfig(clickhouseTable));
    }

    public ClickHouseConfig getConfig(String clickhouseTable)
    {
        String url = String.format("http://%s:%d",
                                   clickhouse.getHost(),
                                   clickhouse.getMappedPort(8123));

        return new ClickHouseConfig(url,
                                    clickhouse.getUsername(),
                                    clickhouse.getPassword(),
                                    clickhouse.getDatabaseName(),
                                    clickhouseTable,
                                    false,
                                    SinkModel.ASYNC_BYTE_BUFFER.name(),
                                    Long.parseLong(ClickHouseConfig.DEFAULT_SOCKET_TIMEOUT));
    }

    public Client getClient(String clickhouseTable)
    {
        ClickHouseConfig config = getConfig(clickhouseTable);
        return new Client.Builder()
                .addEndpoint(config.endpoint)
                .setUsername(config.username)
                .setPassword(config.password)
                .setDefaultDatabase(config.database)
                .build();
    }

    public AbstractFile getFile(String path)
    {
        return new AbstractFile(OutputFormat.fromFileName(path),
                                Paths.get(path),
                                1)
        {
            @Override
            public String getPath()
            {
                return Paths.get("src/test/resources", path).toAbsolutePath().toString();
            }

            @Override
            public AbstractFile next()
            {
                return null;
            }
        };
    }

    public List<List<String>> clickhouseSelect(String table) throws Throwable
    {
        List<List<String>> result = new ArrayList<>();

        try (Client client = getClient(table);
             QueryResponse response = client.query("SELECT * FROM " + table,
                                                   new QuerySettings().setFormat(ClickHouseFormat.TabSeparated)).get())
        {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(response.getInputStream())))
            {
                String line;
                while ((line = reader.readLine()) != null)
                {
                    result.add(new ArrayList<>(Arrays.asList(line.split("\t"))));
                }
            }
        }

        return result;
    }
}
