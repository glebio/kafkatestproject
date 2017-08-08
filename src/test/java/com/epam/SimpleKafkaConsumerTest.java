package com.epam;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

public class SimpleKafkaConsumerTest {

    private MockConsumer<String, String> consumer;

    private static final String TOPIC = "my_topic";

    @BeforeMethod
    public void setUp() {
        consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
    }


    @Test
    public void testSimpleConsumer() {

        consumer.assign(Arrays.asList(new TopicPartition(TOPIC, 0)));

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition(TOPIC, 0), 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        ConsumerRecord<String, String> rec1 = new ConsumerRecord<String, String>(TOPIC, 0,
                0L, "0", "message 0");
        ConsumerRecord<String, String> rec2 = new ConsumerRecord<String, String>(TOPIC, 0,
                1L, "1", "message 1");
        ConsumerRecord<String, String> rec3 = new ConsumerRecord<String, String>(TOPIC, 0,
                2L, "2", "message 2");
        ConsumerRecord<String, String> rec4 = new ConsumerRecord<String, String>(TOPIC, 0,
                3L, "3", "message 3");

        consumer.addRecord(rec1);
        consumer.addRecord(rec2);
        consumer.addRecord(rec3);
        consumer.addRecord(rec4);


        ConsumerRecords<String, String> recs = consumer.poll(1);
        Iterator<ConsumerRecord<String, String>> iter = recs.iterator();

        Assert.assertEquals(iter.next(), rec1);
        Assert.assertEquals(iter.next(), rec2);
        Assert.assertEquals(iter.next(), rec3);
        Assert.assertEquals(iter.next(), rec4);
        Assert.assertFalse(iter.hasNext());

        //method "position" uses get the offset of the next record that will be fetched
        Assert.assertEquals(consumer.position(new TopicPartition(TOPIC, 0)), 4L);
    }
}
