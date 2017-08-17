package com.epam;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class SimpleKafkaConsumer {

    private final static Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaProducer.class);

    private final String topic;

    private Properties properties;

    public Consumer<String, String> myConsumer;

    public SimpleKafkaConsumer(String topic) {
        LOGGER.info("init");
        this.topic = topic;
        properties = new Properties();
        try {
            properties.load(SimpleKafkaConsumer.class.getResourceAsStream("/consumer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        myConsumer = new KafkaConsumer<String, String>(properties);
        myConsumer.subscribe(Collections.singletonList(topic));
        LOGGER.info("init OK");
    }

    ConsumerRecords<String, String> consumeData(long ttl) {
        LOGGER.info("consume messages");
        ConsumerRecords<String, String> records = myConsumer.poll(5_000);
        myConsumer.close();
        return records;
    }

}
