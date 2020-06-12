package com.my.project.jms;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;
import java.util.function.Consumer;

public class MyMessageListener implements MessageListener {

    private static final Logger logger = LoggerFactory.getLogger(MyMessageListener.class);

    private String name;
    private Consumer<String> handleMessage;
    private boolean clientAcknowledge;

    public MyMessageListener(String name, boolean clientAcknowledge) {
        this(name, clientAcknowledge, null);
    }

    public MyMessageListener(String name, boolean acknowledge, Consumer<String> handleMessage) {
        this.name = name;
        this.clientAcknowledge = acknowledge;
        this.handleMessage = handleMessage;
    }

    @Override
    public void onMessage(Message message) {
        try {
            if(clientAcknowledge) {
                message.acknowledge();
            }
            String text = ((TextMessage)message).getText();
            logger.info("[{}] received message: {}", name, text);
            if(handleMessage != null) {
                handleMessage.accept(text);
            }
        } catch (JMSException e) {
            logger.error(e.getMessage(), e);
        }

    }
}
