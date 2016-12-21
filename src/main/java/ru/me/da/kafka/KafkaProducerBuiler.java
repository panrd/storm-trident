package ru.me.da.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by Pavel Popov on 21.12.2016.
 */
public class KafkaProducerBuiler {

    private KafkaProducer producer;

    public KafkaProducerBuiler(String brokers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("producer.type", "async");
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        producer = new KafkaProducer<String, String>(props);
    }

    public void send(String topic, String key, String value) {
        producer.send(new ProducerRecord(topic, key, value));
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
