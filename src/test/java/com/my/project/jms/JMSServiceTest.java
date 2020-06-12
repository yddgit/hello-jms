package com.my.project.jms;

import org.apache.activemq.junit.EmbeddedActiveMQBroker;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit test
 */
public abstract class JMSServiceTest {

    private static final Logger logger = LoggerFactory.getLogger(JMSServiceTest.class);
    private static final String BROKER_URL = "vm://embedded-broker?create=false";
    private static final int TIMEOUT = 1000;
    private static final boolean CLIENT_ACKNOWLEDGE = false;

    @Rule
    public EmbeddedActiveMQBroker broker = new EmbeddedActiveMQBroker() {
        @Override
        protected void configure() {
            getBrokerService().getSystemUsage().getTempUsage().setLimit(1024L * 1024 * 50); // 50MB
        }
    };

    private JMSService jmsService;

    protected abstract JMSService.DestinationType destinationType();
    protected abstract String name();

    @Before
    public void setUp() throws Exception {
        jmsService = new JMSService(BROKER_URL, CLIENT_ACKNOWLEDGE, destinationType());
        logger.info("ClientID: {}", jmsService.getClientID());
    }

    @After
    public void tearDown() throws Exception {
        jmsService.close();
    }

    @Test
    public void normal() throws InterruptedException {
        List<String> messages = Arrays.asList(
            "message0", "message1", "message2", "message3", "message4",
            "message5", "message6", "message7", "message8", "message9"
        );
        // producer
        Runnable produce = () -> jmsService.produce(name(), messages);
        Thread producer = new Thread(produce, "producer");
        // consumer1
        List<String> receivedMessages1 = new ArrayList<>();
        Runnable consume1 = () -> receivedMessages1.addAll(jmsService.consume(name(), TIMEOUT));
        Thread consumer1 = new Thread(consume1, "consumer1");
        // consumer2
        List<String> receivedMessages2 = new ArrayList<>();
        Runnable consume2 = () -> jmsService.consume(name(), TIMEOUT, new MyMessageListener("consumer2", CLIENT_ACKNOWLEDGE, receivedMessages2::add));
        Thread consumer2 = new Thread(consume2, "consumer2");
        // start consumer & producer
        consumer1.start();
        consumer2.start();
        producer.start();
        producer.join();
        consumer1.join();
        consumer2.join();
        // assert
        switch (destinationType()) {
            case QUEUE:
                // Since it is a queue, the messages will be consumed only once by the consumer client in a round robin way.
                receivedMessages1.addAll(receivedMessages2);
                Collections.sort(receivedMessages1);
                System.out.println(receivedMessages1);
                assertEquals(10, receivedMessages1.size());
                assertArrayEquals(messages.toArray(new String[0]), receivedMessages1.toArray(new String[0]));
                break;
            case TOPIC:
                // Moment the topic receives a message, it is consumed by both the consumers.
                assertEquals(10, receivedMessages1.size());
                assertArrayEquals(messages.toArray(new String[0]), receivedMessages1.toArray(new String[0]));
                assertEquals(10, receivedMessages2.size());
                assertArrayEquals(messages.toArray(new String[0]), receivedMessages2.toArray(new String[0]));
                break;
            default:
                fail("unsupported destination type: " + destinationType());
                break;
        }
    }

}
