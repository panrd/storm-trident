package ru.me.da.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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

    public String receiveMessage() throws InterruptedException {

        final Lock messageLock = new ReentrantLock();
        final Condition messageReceivedCondition = messageLock.newCondition();
        final String[] result = new String[1];
        messageLock.lock();
        try {
            open(value -> {
                result[0] = new String(value);
                System.out.println(result[0]);
                messageLock.lock();
                try {
                    messageReceivedCondition.signalAll();
                } finally {
                    messageLock.unlock();
                }
            });

            messageReceivedCondition.await();
        } finally {
            messageLock.unlock();
        }
        return result[0];
    }

    public void open(MessageHandler messageHandler) {
        try {
            Map<String, Integer> topicCountMap = new HashMap<>();
            topicCountMap.put(topic, 1);
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
            executor = Executors.newFixedThreadPool(1);
            for (final KafkaStream<byte[], byte[]> stream : streams) {
                executor.submit(() -> {
                    ConsumerIterator<byte[], byte[]> it = stream.iterator();
                    while (it.hasNext()) {
                        byte[] payload = it.next().message();
                        messageHandler.onMessage(payload);
                    }
                });
            }
            executor.shutdown();
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
