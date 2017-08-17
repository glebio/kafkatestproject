package com.epam;

import org.testng.annotations.Test;

public class MissingElementsFinderTest {

    private static final String TOPIC = "my_topic";

    @Test
    public void testMissingElementFinder() {

        //prepare data for consumer(send data to kafka)
        SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer(TOPIC);
        try {
            simpleKafkaProducer.sendData(10_000, 30, 1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //find missing elements
        MissingElementsFinder finder = new MissingElementsFinder();
        finder.findMissingElementInTopic(TOPIC);
    }

}
