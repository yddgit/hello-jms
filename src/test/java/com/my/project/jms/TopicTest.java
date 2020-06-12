package com.my.project.jms;

public class TopicTest extends JMSServiceTest {
    @Override
    protected JMSService.DestinationType destinationType() {
        return JMSService.DestinationType.TOPIC;
    }
    @Override
    protected String name() {
        return "my-topic";
    }
}
