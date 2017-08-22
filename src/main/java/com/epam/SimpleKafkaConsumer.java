package com.epam;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class SimpleKafkaConsumer {

    private final static Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

    private Properties properties;

    public Consumer<String, String> myConsumer;

    public SimpleKafkaConsumer(String topic) {
        LOGGER.info("init");
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

    List<ConsumerRecord<String, String>> consumeData(long ttl) {
        LOGGER.info("consume messages");
        ConsumerRecords<String, String> records;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        try {
            while (true) {
                records = myConsumer.poll(ttl);
                for (ConsumerRecord<String, String> record : records) {
                    LOGGER.info(" <- message consumed: {offset = {}, key = {}, value = {}}", record.offset(), record.key(), record.value());
                    buffer.add(record);
                }
                myConsumer.commitAsync();
                if (records.isEmpty()) {
                    break;
                }
            }
            myConsumer.commitSync();
        } catch (Exception e) {
            LOGGER.error("Unexpected error " + e);
        } finally {
            LOGGER.info("consumer closed");
            myConsumer.close();
        }
        return buffer;
    }
}
