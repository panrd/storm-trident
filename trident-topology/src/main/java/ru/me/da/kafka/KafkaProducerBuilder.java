package ru.me.da.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by Pavel Popov on 21.12.2016.
 */
public class KafkaProducerBuilder {

    private KafkaProducer<String, String> producer;

    public KafkaProducerBuilder(String brokers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("acks", "all");
        props.put("client.id", "kafkabroker1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("producer.type", "sync");
        props.put("buffer.memory", 33554432);
        props.put("max.request.size", 209715200);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(props);
    }

    public void send(String topic, String key, String value) {
        producer.send(new ProducerRecord<>(topic, key, value));
    }

    public void send(String topic, String value) {
        producer.send(new ProducerRecord<>(topic, value));
    }

    public void close() {
        if (producer != null) {
            producer.close();
        }
    }
}
