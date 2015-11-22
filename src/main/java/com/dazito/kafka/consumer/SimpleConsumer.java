package com.dazito.kafka.consumer;

import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by daz on 22/11/2015.
 */
public class SimpleConsumer {

    private Properties kafkaProperties = new Properties();
    private ConsumerConnector consumerConnector;
    private ConsumerConfig consumerConfig;
    private KafkaStream<String, String> stream;

    private String zookeeperUrl;
    private String groupId;
    private String topic;
    private String waitTime;

    public SimpleConsumer(String zookeeperUrl, String groupId, String topic, String waitTime) {
        this.zookeeperUrl = zookeeperUrl;
        this.groupId = groupId;
        this.topic = topic;
        this.waitTime = waitTime;
    }


    /**
     * Zookeeper to coordinate all the consumers in the consumer group and track the offsets
     */
    public void configure() {
        kafkaProperties.put("zookeeper.connect", zookeeperUrl);
        kafkaProperties.put("group.id", groupId);
        kafkaProperties.put("auto.commit.internal.ms", "1000");
        kafkaProperties.put("auto.offset.reset", "largest");

        // If you want to manually control the offset commit set this config to false
        kafkaProperties.put("auto.commit.enable", "true");

        // By default it waits till infinite otherwise specify a wait time for new messages.
        kafkaProperties.put("consumer.timeout.ms", waitTime);

        consumerConfig = new ConsumerConfig(kafkaProperties);
    }

    public void start() {

        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        /**
         * Define how many threads will read from each topic. We have one topic and one thread.
         *
         * Can be multi thread (one topic 2+ threads). With the high level consumer we usually don't multi thread them.
         * This is because if I want to manually commit offsets it will commit for all the threads that have this
         * consumer instance, at the same time. To avoid this we can create multiple consumer instances and have each
         * of them their own thread.
         */
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);

        /**
         * We will use a decoder to get Kafka to convert messages to Strings
         * valid property will be deserializer.encoding with the charset to use
         * default UTF8 which works for us
         */
        StringDecoder decoder = new StringDecoder(new VerifiableProperties());

        /**
         * Kafka will give us a list of streams of messages for each topic.
         * In this case it is just one topic with a list of a single stream
         */
        stream = consumerConnector.createMessageStreams(topicCountMap, decoder, decoder).get(topic).get(0);

    }

    public String fetchMessage() {
        ConsumerIterator<String, String> it = stream.iterator();

        try {
            return it.next().message();
        }
        catch (ConsumerTimeoutException ex) {
            System.out.println("Waited " + waitTime + " but no messages arrived... Exiting...");
            return null;
        }
    }
}
