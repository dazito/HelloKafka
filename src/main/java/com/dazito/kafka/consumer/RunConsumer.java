package com.dazito.kafka.consumer;

import org.apache.log4j.BasicConfigurator;

/**
 * Created by daz on 22/11/2015.
 */
public class RunConsumer {

    public static void main(String[] args) {
        // log4j init
        BasicConfigurator.configure();

        SimpleConsumer simpleConsumer = new SimpleConsumer("localhost:2181", "myGroupId", "HelloKafka", "10000");
        simpleConsumer.configure();
        simpleConsumer.start();

        String message;

        while ((message = simpleConsumer.fetchMessage()) != null) {

            System.out.println("Received from kafka: " + message);

            /**
             * If you wish to commit offsets on every message, uncomment this line.
             * Best practices is to batch commit offsets (performance wise) which on the other hand may give us problems
             * like if the consumer recovers from a crash it may received messages that he have already been processed,
             * and this is because we did not commit them.
             */
//            myConsumer.consumerConnector.commitOffsets();
        }
    }
}
