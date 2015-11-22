package com.dazito.kafka.producer;

/**
 * Created by daz on 22/11/2015.
 */
public enum ProducerType {

    SYNC("async"),
    ASYNC("sync");

    private final String type;

    ProducerType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
