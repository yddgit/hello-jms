package com.my.project.jms;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 */
public class JMSService implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(JMSService.class);

    private final Connection connection;
    private final DestinationType destinationType;
    /**
     * if session acknowledgeMode is CLIENT_ACKNOWLEDGE
     * client has to explicitly acknowledge each message it receives by calling Message.acknowledge() method
     * else provider may redeliver it to the consumer
     */
    private final boolean clientAcknowledge;

    public JMSService(String brokerURL, boolean clientAcknowledge, DestinationType destinationType) throws JMSException {
        this.destinationType = destinationType;
        this.clientAcknowledge = clientAcknowledge;
        ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerURL);
        connection = connectionFactory.createConnection();
        connection.start();
    }

    public void produce(String name, List<String> messages) {
        doInSession(session -> {
            Destination destination = createDestination(session, name);
            MessageProducer messageProducer = session.createProducer(destination);
            for (String message : messages) {
                TextMessage textMessage = session.createTextMessage(message);
                messageProducer.send(textMessage);
            }
        });
    }

    public void consume(String name, int timeout, MessageListener messageListener) {
        doInSession(session -> {
            Destination destination = createDestination(session, name);
            MessageConsumer consumer = session.createConsumer(destination);
            consumer.setMessageListener(messageListener);
            Thread.sleep(timeout);
        });
    }

    public List<String> consume(String name, int timeout) {
        List<String> receivedMessages = new ArrayList<>();
        doInSession(session -> {
            Destination destination = createDestination(session, name);
            MessageConsumer consumer = session.createConsumer(destination);
            Message message = consumer.receive(timeout);
            while(message != null) {
                if(clientAcknowledge) {
                    message.acknowledge();
                }
                receivedMessages.add(((TextMessage)message).getText());
                message = consumer.receive(timeout);
            }
        });
        return receivedMessages;
    }

    public void doInSession(SessionConsumer consumer) {
        try (Session session = connection.createSession(false, clientAcknowledge ? Session.CLIENT_ACKNOWLEDGE : Session.AUTO_ACKNOWLEDGE)) {
            consumer.doInSession(session);
        } catch (JMSException | InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }

    public Destination createDestination(Session session, String name) throws JMSException {
        Destination destination;
        switch (destinationType) {
            case QUEUE:
                destination = session.createQueue(name);
                break;
            case TOPIC:
                destination = session.createTopic(name);
                break;
            default:
                throw new JMSException("unsupported destination type: " + destinationType);
        }
        return destination;
    }

    public String getClientID() throws JMSException {
        if(connection != null) {
            return this.connection.getClientID();
        }
        return null;
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    public enum DestinationType {
        QUEUE, TOPIC
    }

    @FunctionalInterface
    public interface SessionConsumer {
        /**
         * do JMS operation in a session that will be closed automatically
         * @param session session
         * @throws JMSException
         * @throws InterruptedException
         */
        void doInSession(Session session) throws JMSException, InterruptedException;
    }

}
