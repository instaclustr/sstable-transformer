package com.instaclustr.transformer.core;

import com.google.common.collect.ImmutableMap;
import o.a.c.sidecar.client.shaded.client.HttpClientConfig;
import o.a.c.sidecar.client.shaded.client.SidecarClient;
import o.a.c.sidecar.client.shaded.client.SidecarClientConfig;
import o.a.c.sidecar.client.shaded.client.SidecarClientConfigImpl;
import o.a.c.sidecar.client.shaded.client.SidecarInstanceImpl;
import o.a.c.sidecar.client.shaded.client.SimpleSidecarInstancesProvider;
import o.a.c.sidecar.client.shaded.client.VertxHttpClient;
import o.a.c.sidecar.client.shaded.client.VertxRequestExecutor;
import o.a.c.sidecar.client.shaded.client.retry.ExponentialBackoffRetryPolicy;
import o.a.c.sidecar.client.shaded.client.retry.RetryPolicy;
import o.a.c.sidecar.client.shaded.common.response.RingResponse;
import o.a.c.sidecar.client.shaded.io.vertx.core.Vertx;
import o.a.c.sidecar.client.shaded.io.vertx.core.VertxOptions;
import org.apache.cassandra.spark.data.ReplicationFactor;
import org.apache.cassandra.spark.data.partitioner.CassandraInstance;
import org.apache.cassandra.spark.data.partitioner.CassandraRing;
import org.apache.cassandra.spark.data.partitioner.Partitioner;
import org.apache.cassandra.spark.data.partitioner.TokenPartitioner;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.Mixin;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@Command(name = "partitions",
        description = "Realize how many Spark partitions your Cassandra ring consists of.",
        versionProvider = Transformer.class,
        subcommands = HelpCommand.class)
public class CassandraPartitionsResolver implements Runnable
{
    @Mixin
    public PartitionResolverOptions options;

    public String datacenter;
    public String keyspace;
    public String sidecar;
    public int rf;

    // for picocli
    public CassandraPartitionsResolver()
    {
    }

    public CassandraPartitionsResolver(PartitionResolverOptions options)
    {
        this(options.dc, options.keyspace, options.sidecar, options.rf);
    }

    public CassandraPartitionsResolver(String datacenter, String keyspace, String sidecar, int rf)
    {
        this.datacenter = datacenter;
        this.keyspace = keyspace;
        this.rf = rf;
        this.sidecar = sidecar;
    }

    @Override
    public void run()
    {
        System.out.println(runWithResult().stream().map(Object::toString).collect(joining(",")));
    }

    public List<Integer> runWithResult()
    {
        if (options != null)
        {
            this.datacenter = options.dc;
            this.keyspace = options.keyspace;
            this.sidecar = options.sidecar;
            this.rf = options.rf;
        }

        return Arrays.stream(getPartitions()).boxed().collect(toList());
    }

    public int[] getPartitions()
    {
        String[] split = sidecar.split(":");
        if (split.length != 2)
            throw new TransformerException("Sidecar connection string is not in form of 'hostname:port'");

        String hostname = sidecar.split(":")[0];
        int port;

        try
        {
            port = Integer.parseInt(split[1]);
        }
        catch (NumberFormatException ex)
        {
            throw new TransformerException("Unable to parse port as an integer from input: " + split[1]);
        }

        return getPartitions(datacenter, keyspace, rf, hostname, port);
    }

    private int[] getPartitions(String datacenter, String keyspace, int rf, String hostname, int port)
    {
        try (SidecarClient client = getSidecar(hostname, port))
        {
            RingResponse ring = client.ring(keyspace).get();

            Collection<CassandraInstance> instances = ring
                    .stream()
                    .filter(status -> datacenter.equalsIgnoreCase(status.datacenter()))
                    .map(status -> new CassandraInstance(status.token(), status.fqdn(), status.datacenter()))
                    .collect(toList());

            CassandraRing cassandraRing = new CassandraRing(Partitioner.Murmur3Partitioner,
                                                            keyspace,
                                                            new ReplicationFactor(ReplicationFactor.ReplicationStrategy.NetworkTopologyStrategy,
                                                                                  ImmutableMap.of(datacenter, rf)),
                                                            instances);

            TokenPartitioner tokenPartitioner = new TokenPartitioner(cassandraRing, 1, 1);
            return IntStream.rangeClosed(0, tokenPartitioner.numPartitions() - 1).toArray();
        } catch (Throwable t)
        {
            throw new TransformerException("Unable to get partitions from sidecar at " + hostname + ":" + port, t);
        }
    }

    private SidecarClient getSidecar(String hostname, int port)
    {
        Vertx vertx = Vertx.vertx(new VertxOptions().setWorkerPoolSize(16));

        HttpClientConfig httpClientConfig = new HttpClientConfig.Builder<>()
                .ssl(false)
                .timeoutMillis(10_000)
                .idleTimeoutMillis(30_000)
                .receiveBufferSize(8196)
                .maxChunkSize(4092)
                .userAgent("sample-agent/0.0.1")
                .build();

        SidecarClientConfig clientConfig = SidecarClientConfigImpl.builder()
                .maxRetries(10)
                .retryDelayMillis(200)
                .maxRetryDelayMillis(10_000)
                .build();

        RetryPolicy defaultRetryPolicy = new ExponentialBackoffRetryPolicy(clientConfig.maxRetries(),
                                                                           clientConfig.retryDelayMillis(),
                                                                           clientConfig.maxRetryDelayMillis());

        VertxHttpClient vertxHttpClient = new VertxHttpClient(vertx, httpClientConfig);
        VertxRequestExecutor requestExecutor = new VertxRequestExecutor(vertxHttpClient);
        SimpleSidecarInstancesProvider instancesProvider = new SimpleSidecarInstancesProvider(List.of(new SidecarInstanceImpl(hostname, port)));
        return new SidecarClient(instancesProvider, requestExecutor, clientConfig, defaultRetryPolicy);
    }
}
