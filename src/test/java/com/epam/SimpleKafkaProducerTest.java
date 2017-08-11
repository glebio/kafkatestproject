package com.epam;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class SimpleKafkaProducerTest {

    private MockProducer<String, String> producer;

    private static final String TOPIC = "my_topic";

    @BeforeMethod
    public void setUp() {
        producer = new MockProducer<String, String>(true, new StringSerializer(), new StringSerializer());
    }

    @Test
    public void testProducer() throws IOException {
        SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer(TOPIC);
        simpleKafkaProducer.myProducer = producer;

        try {
            simpleKafkaProducer.sendData(5_000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        List<ProducerRecord<String, String>> actual = producer.history();

        List<ProducerRecord<String, String>> expected = Arrays.asList(
                new ProducerRecord<String, String>(TOPIC, "1", "message 1"),
                new ProducerRecord<String, String>(TOPIC, "2", "message 2"),
                new ProducerRecord<String, String>(TOPIC, "3", "message 3"));

        Assert.assertEquals(actual, expected);
    }

}
