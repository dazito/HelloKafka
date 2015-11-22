package com.dazito.kafka.producer;

import org.apache.log4j.BasicConfigurator;

/**
 * Created by daz on 22/11/2015.
 */
public class RunProducer {

    public static void main(String[] args) {
        // log4j init
        BasicConfigurator.configure();

        SimpleProducer simpleProducer = new SimpleProducer("localhost:9092", "HelloKafka");
        simpleProducer.configureAndStart(ProducerType.ASYNC);

        for(int i = 0; i<100; i++) {
            simpleProducer.produceMessage("Hello Kafka - This is message number " + i);
        }

        simpleProducer.close();
    }
}
