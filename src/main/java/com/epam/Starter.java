package com.epam;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Starter {

    private final static Logger LOGGER = LoggerFactory.getLogger(Starter.class);

    private static final String TOPIC = "my_topic";

    public static void main(String[] args) {

        SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer(TOPIC);
        try {
            simpleKafkaProducer.sendData(5_000, 10, 1);
        } catch (Exception ex) {
            LOGGER.error("", ex);
        }


        SimpleKafkaConsumer simpleKafkaConsumer = new SimpleKafkaConsumer(TOPIC);
        try {
            simpleKafkaConsumer.consumeData(5_000);
        } catch (Exception ex) {
            LOGGER.error("", ex);
        }

    }
}
