/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.instaclustr.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import io.vertx.core.http.HttpMethod;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.sidecar.common.server.utils.MillisecondBoundConfiguration;
import org.apache.cassandra.sidecar.config.SchemaReportingConfiguration;
import org.apache.cassandra.sidecar.config.yaml.SidecarConfigurationImpl;
import org.apache.cassandra.sidecar.server.Server;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.sidecar.testing.SharedClusterIntegrationTestBase;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.apache.cassandra.testing.TestUtils.DC1_RF1;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.TEST_TABLE_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * copy copied from core for simplicity, but also enriches it to start ClickHouse so we
 * have Cassandra node via dtest api machinery + ClickHouse node in Docker.
 * <p>
 * In tests, we start Cassandra (with Sidecar), create a table, insert some data, then
 * we run the transformer with a sink which will pour data to ClickHouse, then we verify
 * it is in ClickHouse, so we do the full-on integration test.
 */
public class AbstractIntegrationTest extends SharedClusterIntegrationTestBase
{
    static
    {
        String cassandraVersion = System.getProperty("transformer.cassandra.version");

        if (cassandraVersion != null && cassandraVersion.equals("4.1"))
            System.setProperty("cassandra.sidecar.versions_to_test", "4.1");
        else
            System.setProperty("cassandra.sidecar.versions_to_test", "5.0");

        System.setProperty("cassandra.test.dtest_jar_path", "dtest-jars");
        System.setProperty("cassandra.integration.sidecar.test.enable_mtls", "false");
        System.setProperty("SKIP_STARTUP_VALIDATIONS", "true");
    }

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractIntegrationTest.class);

    protected final List<Server> sidecarServerList = new ArrayList<>();

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF1);
        createTestTable(new QualifiedName(TEST_KEYSPACE, TEST_TABLE_PREFIX),
                        "CREATE TABLE %s (id int primary key)");

        try (Cluster cluster = SharedClusterIntegrationTestBase.createDriverCluster(super.cluster))
        {
            Session session = cluster.connect();
            for (int i = 0; i < 10_000; i++)
            {
                if (i % 1000 == 0)
                    super.cluster.get(1).flush(TEST_KEYSPACE);

                session.execute(String.format("INSERT INTO %s.%s (id) values (%s)", TEST_KEYSPACE, TEST_TABLE_PREFIX, i));
            }
        }

        cluster.get(1).flush(TEST_KEYSPACE);
    }

    @Override
    protected void startSidecar(ICluster<? extends IInstance> cluster) throws InterruptedException
    {
        for (IInstance instance : cluster)
        {
            LOGGER.info("Starting Sidecar instance for Cassandra instance {}",
                        instance.config().num());
            Server server = startSidecarWithInstances(Collections.singleton(instance));
            sidecarServerList.add(server);
        }

        assertThat(sidecarServerList.size())
                .as("Each Cassandra Instance will be managed by a single Sidecar instance")
                .isEqualTo(cluster.size());

        // assign the server to the first instance
        server = sidecarServerList.get(0);
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                .tokenCount(16)
                .nodesPerDc(1);
    }

    @Override
    protected Function<SidecarConfigurationImpl.Builder, SidecarConfigurationImpl.Builder> configurationOverrides()
    {
        return builder ->
        {
            builder.schemaReportingConfiguration(new SchemaReportingConfiguration()
            {
                @Override
                public String endpoint()
                {
                    return null;
                }

                @Override
                public HttpMethod method()
                {
                    return null;
                }

                @Override
                public int maxRetries()
                {
                    return 0;
                }

                @Override
                public MillisecondBoundConfiguration retryDelay()
                {
                    return new MillisecondBoundConfiguration("1m");
                }

                @Override
                public boolean enabled()
                {
                    return false;
                }

                @Override
                public @NotNull MillisecondBoundConfiguration initialDelay()
                {
                    return new MillisecondBoundConfiguration("1s");
                }

                @Override
                public @NotNull MillisecondBoundConfiguration executeInterval()
                {
                    return new MillisecondBoundConfiguration("1s");
                }
            });
            return builder;
        };
    }
}
