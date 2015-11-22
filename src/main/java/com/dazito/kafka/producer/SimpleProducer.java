package com.dazito.kafka.producer;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * Created by daz on 22/11/2015.
 */
public class SimpleProducer {

    private final String brokerList;
    private final String topic;

    private Properties kafkaProperties;

    // We will be using String messages
    private Producer<String, String> producer;
    private ProducerConfig producerConfig;


    public SimpleProducer(String brokerList, String topic) {
        this.brokerList = brokerList;
        this.topic = topic;
    }

    // Set the producer configuration
    public void configureAndStart(ProducerType producerType) {
        kafkaProperties = new Properties();
        kafkaProperties.put("metadata.broker.list", brokerList);
        kafkaProperties.put("serializer.class", "kafka.serializer.StringEncoder");
        kafkaProperties.put("request.required.acks", "1");
        kafkaProperties.put("producer.type", producerType.getType());

        producerConfig = new ProducerConfig(kafkaProperties);

        // Start the producer
        producer = new Producer<String, String>(producerConfig);
    }

    // Close
    public void close() {
        producer.close();
    }

    // Create the message and send it over to kafka
    // We use null for the key which means the message will be sent to a random partition
    // The producer will switch over to a different random partition every 10 minutes
    public void produceMessage(String message) {
        KeyedMessage<String, String> keyedMessage = new KeyedMessage<String, String>(topic, null, message);
        producer.send(keyedMessage);
    }
}
