package ru.me.da.kafka;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.*;
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
        props.put("auto.offset.reset", "smallest");

        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
        this.topic = topic;
    }

    public List<String> receiveMessages() throws InterruptedException {

        Lock messageLock = new ReentrantLock();
        Condition messageReceivedCondition = messageLock.newCondition();
        List<String> messages = new ArrayList<>();
        messageLock.lock();
        try {
            open(value -> {
                messages.add(new String(value));
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
        return messages;
    }

    public void open(MessageHandler messageHandler) {
        try {
            Map<String, Integer> topicCountMap = new HashMap<>();
            topicCountMap.put(topic, 1);
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
            List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
            executor = Executors.newSingleThreadExecutor();
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
