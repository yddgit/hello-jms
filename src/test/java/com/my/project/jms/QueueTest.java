package com.my.project.jms;

public class QueueTest extends JMSServiceTest {
    @Override
    protected JMSService.DestinationType destinationType() {
        return JMSService.DestinationType.QUEUE;
    }
    @Override
    protected String name() {
        return "my-queue";
    }
}
