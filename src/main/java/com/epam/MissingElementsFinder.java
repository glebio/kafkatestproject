package com.epam;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Random;
import java.util.TreeSet;

public class MissingElementsFinder {

    private final static Logger LOGGER = LoggerFactory.getLogger(MissingElementsFinder.class);


    public void findMissingElementInTopic(String topic) {

        TreeSet<Integer> set = new TreeSet<>(new Comparator<Integer>() {
            @Override
            public int compare(Integer o1, Integer o2) {
                return o1.compareTo(o2);
            }
        });

        SimpleKafkaConsumer myTestConsumer = new SimpleKafkaConsumer(topic);

        ConsumerRecords<String, String> actualRecords = myTestConsumer.consumeData(5_000);

        for (ConsumerRecord<String, String> record : actualRecords) {
            set.add(Integer.parseInt(record.key()));
        }

        LOGGER.info("Elements which present in topic: " + set);

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


    }
}
