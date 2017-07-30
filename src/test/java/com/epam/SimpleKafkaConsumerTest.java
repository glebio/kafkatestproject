package com.epam;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;

public class SimpleKafkaConsumerTest {

    private MockConsumer<String, String> consumer;

    private static final String TOPIC = "my_topic";

    @BeforeMethod
    public void setUp() {
        consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
    }

    @Test
    public void testConsumer() throws IOException {
        SimpleKafkaConsumer myTestConsumer = new SimpleKafkaConsumer(TOPIC);
        myTestConsumer.myConsumer = consumer;

        consumer.assign(Arrays.asList(new TopicPartition("my_topic", 0)));

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("my_topic", 0), 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        consumer.addRecord(new ConsumerRecord<String, String>("my_topic", 0,
                0L, "0", "message 0"));
        consumer.addRecord(new ConsumerRecord<String, String>("my_topic", 0,
                1L, "1", "message 1"));
        consumer.addRecord(new ConsumerRecord<String, String>("my_topic", 0,
                2L, "2", "message 1"));
        consumer.addRecord(new ConsumerRecord<String, String>("my_topic", 0,
                3L, "3", "message 3"));
        consumer.addRecord(new ConsumerRecord<String, String>("my_topic", 0,
                4L, "4", "message 4"));

        myTestConsumer.consumeData(5_000);

    }
}
