package com.epam;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

public class SimpleKafkaConsumerTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(SimpleKafkaConsumer.class);

    private MockConsumer<String, String> consumer;

    private static final String TOPIC = "my_topic";

    public List<ConsumerRecord<String, String>> expected;


    @BeforeMethod
    public void setUp() {
        consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
    }


    @Test
    public void testSimpleConsumer() {

        SimpleKafkaConsumer myTestConsumer = new SimpleKafkaConsumer(TOPIC);
        myTestConsumer.myConsumer = consumer;

        //prepare "expected" data
        expected = Arrays.asList(
                new ConsumerRecord<String, String>(TOPIC, 0,
                        0L, "0", "message 0"),
                new ConsumerRecord<String, String>(TOPIC, 0,
                        1L, "1", "message 1"),
                new ConsumerRecord<String, String>(TOPIC, 0,
                        2L, "2", "message 2"));

        //prepare mock data
        TopicPartition topicPartition = new TopicPartition(TOPIC, 0);
        consumer.assign(Collections.singletonList(topicPartition));

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(topicPartition, 0L);
        consumer.updateBeginningOffsets(beginningOffsets);


        consumer.addRecord(expected.get(0));
        consumer.addRecord(expected.get(1));
        consumer.addRecord(expected.get(2));


        //execution consumeData method
        List<ConsumerRecord<String, String>> actualRecords = myTestConsumer.consumeData(5_000);

        assertThat(actualRecords).containsAll(expected);

    }






}
