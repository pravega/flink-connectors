/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.utils;

import io.pravega.client.ClientConfig;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.impl.Controller;
import io.pravega.client.stream.impl.ControllerImpl;
import io.pravega.client.stream.impl.ControllerImplConfig;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.local.InProcPravegaCluster;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import com.google.common.base.Preconditions;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.concurrent.NotThreadSafe;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Utility functions for creating the test setup.
 */
@Slf4j
@NotThreadSafe
public final class SetupUtils {

    private static final ScheduledExecutorService DEFAULT_SCHEDULED_EXECUTOR_SERVICE = ExecutorServiceHelpers.newScheduledThreadPool(3, "SetupUtils");

    private final PravegaGateway gateway;

    // Manage the state of the class.
    private final AtomicBoolean started = new AtomicBoolean(false);

    // The test Scope name.
    @Getter
    private final String scope = RandomStringUtils.randomAlphabetic(20);

    public SetupUtils() {
        this(System.getProperty("pravega.uri"));
    }

    public SetupUtils(String externalUri) {
        if (externalUri != null) {
            log.info("Using Pravega services at {}.", externalUri);
            gateway = new ExternalPravegaGateway(URI.create(externalUri));
        } else {
            log.info("Starting in-process Pravega services.");
            gateway = new InProcPravegaGateway();
        }
    }

    /**
     * Start all pravega related services required for the test deployment.
     *
     * @throws Exception on any errors.
     */
    public void startAllServices() throws Exception {
        if (!this.started.compareAndSet(false, true)) {
            log.warn("Services already started, not attempting to start again");
            return;
        }

        gateway.start();
    }

    /**
     * Stop the pravega cluster and release all resources.
     *
     * @throws Exception on any errors.
     */
    public void stopAllServices() throws Exception {
        if (!this.started.compareAndSet(true, false)) {
            log.warn("Services not yet started or already stopped, not attempting to stop");
            return;
        }

        try {
            gateway.stop();
        } catch (Exception e) {
            log.warn("Services did not stop cleanly (" + e.getMessage() + ")", e);
        }
    }

    /**
     * Fetch the controller endpoint for this cluster.
     *
     * @return URI The controller endpoint to connect to this cluster.
     */
    public URI getControllerUri() {
        return getClientConfig().getControllerURI();
    }

    /**
     * Fetch the client configuration with which to connect to the controller.
     */
    public ClientConfig getClientConfig() {
        return this.gateway.getClientConfig();
    }

    /**
     * Fetch the {@link PravegaConfig} for integration test purposes.
     */
    public PravegaConfig getPravegaConfig() {
        return PravegaConfig.fromDefaults().withControllerURI(getControllerUri()).withDefaultScope(getScope());
    }

    /**
     * Create a controller facade for this cluster.
     * @return The controller facade, which must be closed by the caller.
     */
    public Controller newController() {
        ControllerImplConfig config = ControllerImplConfig.builder()
                .clientConfig(getClientConfig())
                .build();
        return new ControllerImpl(config, DEFAULT_SCHEDULED_EXECUTOR_SERVICE);
    }

    /**
     * Create a {@link ClientFactory} for this cluster and scope.
     */
    public ClientFactory newClientFactory() {
        return ClientFactory.withScope(this.scope, getControllerUri());
    }

    /**
     * Create the test stream.
     *
     * @param streamName     Name of the test stream.
     * @param numSegments    Number of segments to be created for this stream.
     *
     * @throws Exception on any errors.
     */
    public void createTestStream(final String streamName, final int numSegments)
            throws Exception {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);
        Preconditions.checkArgument(numSegments > 0);

        @Cleanup
        StreamManager streamManager = StreamManager.create(getControllerUri());
        streamManager.createScope(this.scope);
        streamManager.createStream(this.scope, streamName,
                StreamConfiguration.builder()
                        .scope(this.scope)
                        .streamName(streamName)
                        .scalingPolicy(ScalingPolicy.fixed(numSegments))
                        .build());
        log.info("Created stream: " + streamName);
    }

    /**
     * Create a stream writer for writing Integer events.
     *
     * @param streamName    Name of the test stream.
     *
     * @return Stream writer instance.
     */
    public EventStreamWriter<Integer> getIntegerWriter(final String streamName) {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);

        ClientFactory clientFactory = ClientFactory.withScope(this.scope, getControllerUri());
        return clientFactory.createEventWriter(
                streamName,
                new IntegerSerializer(),
                EventWriterConfig.builder().build());
    }

    /**
     * Create a stream reader for reading Integer events.
     *
     * @param streamName    Name of the test stream.
     *
     * @return Stream reader instance.
     */
    public EventStreamReader<Integer> getIntegerReader(final String streamName) {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);

        ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(this.scope, getControllerUri());
        final String readerGroup = "testReaderGroup" + this.scope + streamName;
        readerGroupManager.createReaderGroup(
                readerGroup,
                ReaderGroupConfig.builder().stream(Stream.of(this.scope, streamName)).build());

        ClientFactory clientFactory = ClientFactory.withScope(this.scope, getControllerUri());
        final String readerGroupId = UUID.randomUUID().toString();
        return clientFactory.createReader(
                readerGroupId,
                readerGroup,
                new IntegerSerializer(),
                ReaderConfig.builder().build());
    }

    /**
     * A gateway interface to Pravega for integration test purposes.
     */
    private interface PravegaGateway {
        /**
         * Starts the gateway.
         */
        void start() throws Exception;

        /**
         * Stops the gateway.
         */
        void stop() throws Exception;

        /**
         * Gets the client configuration with which to connect to the controller.
         */
        ClientConfig getClientConfig();
    }

    static class InProcPravegaGateway implements PravegaGateway {

        // The pravega cluster.
        private InProcPravegaCluster inProcPravegaCluster = null;

        @Override
        public void start() throws Exception {
            int zkPort = TestUtils.getAvailableListenPort();
            int controllerPort = TestUtils.getAvailableListenPort();
            int hostPort = TestUtils.getAvailableListenPort();
            int restPort = TestUtils.getAvailableListenPort();

            this.inProcPravegaCluster = InProcPravegaCluster.builder()
                    .isInProcZK(true)
                    .zkUrl("localhost:" + zkPort)
                    .zkPort(zkPort)
                    .isInMemStorage(true)
                    .isInProcController(true)
                    .controllerCount(1)
                    .restServerPort(restPort)
                    .isInProcSegmentStore(true)
                    .segmentStoreCount(1)
                    .containerCount(4)
                    .enableTls(false).keyFile("").certFile("").enableAuth(false).userName("").passwd("") // pravega#2519
                    .build();
            this.inProcPravegaCluster.setControllerPorts(new int[]{controllerPort});
            this.inProcPravegaCluster.setSegmentStorePorts(new int[]{hostPort});
            this.inProcPravegaCluster.start();
            log.info("Initialized Pravega Cluster");
            log.info("Controller port is {}", controllerPort);
            log.info("Host port is {}", hostPort);
            log.info("REST server port is {}", restPort);
        }

        @Override
        public void stop() throws Exception {
            inProcPravegaCluster.close();
        }

        @Override
        public ClientConfig getClientConfig() {
            return ClientConfig.builder()
                    .controllerURI(URI.create(inProcPravegaCluster.getControllerURI()))
                    .build();
        }
    }

    static class ExternalPravegaGateway implements PravegaGateway {

        private final URI controllerUri;

        public ExternalPravegaGateway(URI controllerUri) {
            this.controllerUri = controllerUri;
        }

        @Override
        public void start() throws Exception {
        }

        @Override
        public void stop() throws Exception {
        }

        @Override
        public ClientConfig getClientConfig() {
            return ClientConfig.builder()
                    .controllerURI(controllerUri)
                    .build();
        }
    }
}