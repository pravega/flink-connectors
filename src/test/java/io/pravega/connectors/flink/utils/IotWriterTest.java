package io.pravega.connectors.flink.utils;

import io.pravega.client.ClientFactory;
import io.pravega.client.stream.impl.Controller;
import io.pravega.connectors.flink.util.StreamId;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

@Slf4j
public class IotWriterTest {
    // Setup utility.
    private static final SetupUtils SETUP_UTILS = new SetupUtils();

    private Controller controllerClient;
    private ClientFactory clientFactory;
    private IotWriter producer;

    @BeforeClass
    public static void setupPravega() throws Exception {
        SETUP_UTILS.startAllServices();
    }

    @AfterClass
    public static void tearDownPravega() throws Exception {
        SETUP_UTILS.stopAllServices();
    }

    @Before
    public void before() throws Exception {
        controllerClient = SETUP_UTILS.newController();
        clientFactory = SETUP_UTILS.newClientFactory();
    }

    @After
    public void after() throws Exception {
        if (clientFactory != null) {
            clientFactory.close();
        }
    }


    @Test
    public void testScaling() throws Exception {
        // set up the stream
        final StreamId streamId = new StreamId(SETUP_UTILS.getScope(), RandomStringUtils.randomAlphabetic(20));
        SETUP_UTILS.createTestStream(streamId.getName(), 2);
        IotWriter writer = new IotWriter(clientFactory, controllerClient, streamId);
        writer.start().join();
        writer.close();

    }

}