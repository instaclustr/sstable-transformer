package com.instaclustr.cassandra;

import com.instaclustr.transformer.core.CassandraPartitionsResolver;
import com.instaclustr.transformer.core.PartitionResolverOptions;
import org.apache.cassandra.sidecar.testing.QualifiedName;
import org.apache.cassandra.testing.ClusterBuilderConfiguration;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.apache.cassandra.testing.TestUtils.DC1_RF3;
import static org.apache.cassandra.testing.TestUtils.TEST_KEYSPACE;
import static org.apache.cassandra.testing.TestUtils.TEST_TABLE_PREFIX;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CassandraPartitionsResolverTest extends AbstractIntegrationTest
{
    @Test
    public void testPartitionsResolving()
    {
        PartitionResolverOptions options = new PartitionResolverOptions();
        options.dc = "datacenter1";
        options.keyspace = TEST_KEYSPACE;
        options.rf = 3;
        options.sidecar = "localhost:" + server.actualPort();
        List<Integer> partitions = new CassandraPartitionsResolver(options).runWithResult();
        assertTrue(partitions.size() >= 48);
    }

    @Override
    protected void initializeSchemaForTest()
    {
        createTestKeyspace(TEST_KEYSPACE, DC1_RF3);
        createTestTable(new QualifiedName(TEST_KEYSPACE, TEST_TABLE_PREFIX),
                        "CREATE TABLE %s (id int primary key)");

        // we do not need to create any data
    }

    @Override
    protected ClusterBuilderConfiguration testClusterConfiguration()
    {
        return super.testClusterConfiguration()
                .tokenCount(16)
                .nodesPerDc(3);
    }
}
