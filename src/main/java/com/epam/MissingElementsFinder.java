package com.epam;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.TreeSet;

public class MissingElementsFinder {

    private final static Logger LOGGER = LoggerFactory.getLogger(MissingElementsFinder.class);

    public void findMissingElementInTopic(String topic) {

        TreeSet<Integer> set = new TreeSet<>();

        SimpleKafkaConsumer myTestConsumer = new SimpleKafkaConsumer(topic);

        List<ConsumerRecord<String, String>> actualRecords = myTestConsumer.consumeData(5_000);

        for (ConsumerRecord<String, String> record : actualRecords) {
            set.add(Integer.parseInt(record.key()));
        }

        LOGGER.info("Elements which present in topic: " + set);

        if (set.size() != 0) {
            int num = set.first();
            for (Integer i : set) {
                if (num == i) {
                    continue;
                }
                if (num + 1 != i) {
                    if (num + 2 != i) {
                        LOGGER.info("Missing elements from " + (num + 1) + " to " + (i - 1));
                        num = i;
                    } else {
                        LOGGER.info("Missing element " + (num + 1));
                        num = i;
                    }
                } else {
                    num = i;
                }
            }

        } else {
            System.out.println("Sorry, but collection didn't fill!");
        }
    }
}
