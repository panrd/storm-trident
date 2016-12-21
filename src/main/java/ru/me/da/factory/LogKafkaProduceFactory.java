package ru.me.da.factory;

import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.trident.selector.DefaultTopicSelector;
import ru.me.da.util.StreamField;

import java.util.Properties;

/**
 * Created by Pavel Popov on 22.11.2016.
 */
public class LogKafkaProduceFactory extends TridentKafkaStateFactory {

    public LogKafkaProduceFactory build() {

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "192.168.173.131:9092");
        producerProps.put("acks", "1");
        producerProps.put("retries", 0);
        producerProps.put("batch.size", 16384);
        producerProps.put("linger.ms", 1);
        producerProps.put("buffer.memory", 33554432);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        withProducerProperties(producerProps)
                .withKafkaTopicSelector(new DefaultTopicSelector("alert-topic"))
                .withTridentTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper(StreamField.LEVEL_STR, StreamField.ERROR_JSON_STR));
        return this;
    }

}
