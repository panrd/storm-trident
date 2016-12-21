package ru.me.da.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

/**
 * Created by Pavel Popov on 21.12.2016.
 */
public class KafkaConsumerBuilder {

    private String zkHost;
    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;

    public KafkaConsumerBuilder(String zkHost, String topic) {
        this.zkHost = zkHost;

        Properties props = new Properties();
        props.put("zookeeper.connect", zkHost);
        props.put("group.id", "zk_gr");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        this.topic = topic;
    }
}
