package ru.me.da;

import kafka.admin.AdminUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

/**
 * Created by Pavel Popov on 21.12.2016.
 */
public class KafkaBuilder {


    public static KafkaProducer getProducer(String brokers) {
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
        props.put("partitioner.class", "PartitionerTest");

        KafkaProducer producer = new KafkaProducer<String, String>(props);

        return producer;
    }

    public static KafkaConsumer getConsumer(String zkHost) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zkHost);
        props.put("group.id", "kf_gr");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        AdminUtils.topicExists()
        new ConsumerConfig(props);
        //consumer = Consumer.createJavaConsumerConnector(createConsumerConfig(a_zookeeper, a_groupId));

        return null;
    }


}
