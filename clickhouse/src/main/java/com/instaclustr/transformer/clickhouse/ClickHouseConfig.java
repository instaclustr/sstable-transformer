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
    public static final String ENDPOINT_PROPERTY = "endpoint";
    public static final String USERNAME_PROPERTY = "username";
    public static final String PASSWORD_PROPERTY = "password";
    public static final String DATABASE_PROPERTY = "database";
    public static final String TABLE_PROPERTY = "table";
    public static final String REMOVE_FILE_AFTER_PROCESSED_PROPERTY = "remove_file_after_processed";
    public static final String SINK_MODEL_PROPERTY = "sink_model";
    public static final String SOCKET_TIMEOUT_PROPERTY = "socket_timeout";

    public static final String DEFAULT_ENDPOINT = "http://127.0.0.1:8123";
    public static final String DEFAULT_USERNAME = "clickhouse";
    public static final String DEFAULT_PASSWORD = "clickhouse";
    public static final String DEFAULT_DATABASE = "default";
    public static final String DEFAULT_REMOVE_FILE_AFTER_PROCESSED = "false";
    public static final String DEFAULT_SINK_MODEL = "async_byte_buffer";
    public static final String DEFAULT_SOCKET_TIMEOUT = "300000";

    public final String endpoint;
    public final String username;
    public final String password;
    public final String table;
    public final boolean removeFileAfterProcessed;
    public final SinkModel sinkModel;
    public final String database;
    public final long socketTimeout;

    public ClickHouseConfig(String endpoint,
                            String username,
                            String password,
                            String database,
                            String table,
                            boolean removeFileAfterProcessed,
                            String sinkModel,
                            long socketTimeout)
    {
        this.endpoint = endpoint;
        this.username = username;
        this.password = password;
        this.database = database;

        if (table == null)
            throw new IllegalArgumentException("'" + TABLE_PROPERTY + "' property has to be specified.");

        this.table = table;

        this.removeFileAfterProcessed = removeFileAfterProcessed;
        this.socketTimeout = socketTimeout;

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
             properties.getProperty(SINK_MODEL_PROPERTY, DEFAULT_SINK_MODEL),
             Long.parseLong(properties.getProperty(SOCKET_TIMEOUT_PROPERTY, DEFAULT_SOCKET_TIMEOUT)));
    }
}
