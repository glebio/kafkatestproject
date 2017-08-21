package com.epam;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;

public class SimpleKafkaProducer {

    private final static Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaProducer.class);

    private final String topic;

    private Properties properties;

    public Producer<String, String> myProducer;

    public SimpleKafkaProducer(String topic) {
        LOGGER.info("init");
        this.topic = topic;
        properties = new Properties();
        try {
            properties.load(SimpleKafkaProducer.class.getResourceAsStream("/producer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        myProducer = new KafkaProducer<>(properties);
        LOGGER.info("init OK");
    }

    void sendData(int max, int min) throws InterruptedException {
        LOGGER.info("send messages");
        Random rand = new Random();
        for (int i = 0; i < max; i++) {
            int randomNum = rand.nextInt((max - min) + 1) + min;
            String key = Integer.toString(randomNum);
            String value = "message " + key;
            myProducer.send(new ProducerRecord<>(topic, key, value));
            LOGGER.info(" -> message sent: {key = {}, value = {}}", key, value);
            //Thread.sleep(ttl / 50);
        }
        myProducer.close();
        LOGGER.info("send messages done");
        LOGGER.info("producer closed");
    }


}
