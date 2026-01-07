package com.instaclustr.transformer.clickhouse;

import com.instaclustr.transformer.api.SinkModel;

import java.util.Arrays;
import java.util.Properties;

import static com.instaclustr.transformer.api.SinkModel.valueOf;
import static com.instaclustr.transformer.api.SinkModel.values;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class ClickHouseConfig
{
    private static final String ENDPOINT_PROPERTY = "endpoint";
    private static final String USERNAME_PROPERTY = "username";
    private static final String PASSWORD_PROPERTY = "password";
    private static final String DATABASE_PROPERTY = "database";
    private static final String TABLE_PROPERTY = "table";
    private static final String REMOVE_FILE_AFTER_PROCESSED_PROPERTY = "remove_file_after_processed";
    private static final String SINK_MODEL_PROPERTY = "sink_model";
    private static final String BUFFER_SIZE_PROPERTY = "buffer_size";

    private static final String DEFAULT_ENDPOINT = "http://127.0.0.1:8123";
    private static final String DEFAULT_USERNAME = "clickhouse";
    private static final String DEFAULT_PASSWORD = "clickhouse";
    private static final String DEFAULT_DATABASE = "default";
    private static final String DEFAULT_REMOVE_FILE_AFTER_PROCESSED = "false";
    private static final String DEFAULT_SINK_MODEL = "byte_buffer";

    public String endpoint;
    public String username;
    public String password;
    public String table;
    public boolean removeFileAfterProcessed;
    public SinkModel sinkModel;
    public String database;

    public ClickHouseConfig(String endpoint,
                            String username,
                            String password,
                            String database,
                            String table,
                            boolean removeFileAfterProcessed,
                            String sinkModel)
    {
        this.endpoint = endpoint;
        this.username = username;
        this.password = password;
        this.database = database;

        if (table == null)
            throw new IllegalArgumentException("'" + TABLE_PROPERTY + "' property has to be specified.");

        this.table = table;

        this.removeFileAfterProcessed = removeFileAfterProcessed;

        try
        {
            this.sinkModel = valueOf(sinkModel.toUpperCase());
        } catch (Throwable t)
        {
            throw new IllegalArgumentException(format("%s has to be one of %s but it is '%s'",
                                                      SINK_MODEL_PROPERTY,
                                                      Arrays.stream(values()).collect(toList()),
                                                      sinkModel));
        }
    }

    public ClickHouseConfig(Properties properties)
    {
        this(properties.getProperty(ENDPOINT_PROPERTY, DEFAULT_ENDPOINT),
             properties.getProperty(USERNAME_PROPERTY, DEFAULT_USERNAME),
             properties.getProperty(PASSWORD_PROPERTY, DEFAULT_PASSWORD),
             properties.getProperty(DATABASE_PROPERTY, DEFAULT_DATABASE),
             properties.getProperty(TABLE_PROPERTY),
             Boolean.parseBoolean(properties.getProperty(REMOVE_FILE_AFTER_PROCESSED_PROPERTY,
                                                         DEFAULT_REMOVE_FILE_AFTER_PROCESSED)),
             properties.getProperty(SINK_MODEL_PROPERTY, DEFAULT_SINK_MODEL));
    }
}
