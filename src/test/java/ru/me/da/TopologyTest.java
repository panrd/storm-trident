package ru.me.da;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import ru.me.da.builder.TridentTopologyBuilder;
import ru.me.da.kafka.KafkaAdmin;
import ru.me.da.kafka.KafkaConsumerBuilder;
import ru.me.da.kafka.KafkaProducerBuilder;
import ru.me.da.model.LogMessage;
import ru.me.da.model.LogRate;
import ru.me.da.util.Const;
import ru.me.da.util.Utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Created by Pavel Popov on 05.12.2016.
 */
public class TopologyTest {

    private static final Logger logger = LoggerFactory.getLogger(TopologyTest.class);

    private static String alertTopic = "alert-topic";
    private static String logTopic = "log-topic";
    private static String testTopic = "test-topic";

    private static String kafkaBrokers = "hbasehost:9092";
    private static String zk = "hbasehost:2185";

    private static String topologyName = "trident-test-topology";

//    private KafkaProducerBuilder getKafkaProdBuilder() {
//        logger.info("Creating a producer for kafka brokers: {}", kafkaBrokers);
//        return KafkaProducerBuilder.aProducer()
//                .withType(KafkaProducer.TYPE_ASYNC)
//                .withBrokerList(kafkaBrokers)
//                .withKeySerializer("kafka.serializer.StringEncoder")
//                .withMessageSerializer("kafka.serializer.StringEncoder");
//    }
//
//    private KafkaMessage receiveKafkaMessage(String topic) throws InterruptedException {
//        final KafkaConsumerSession consumerSession = aSession()
//                .withTopic(topic)
//                .withGroup("group_iq")
//                .withZookeeperConnect(zk)
//                .withAutoOffsetReset(OFFSET_SMALLEST)
//                .withParallelism(1).build();
//
//        final KafkaMessage[] kafkaMessageHolder = new KafkaMessage[1];
//        final Lock messageLock = new ReentrantLock();
//        final Condition messageReceivedCondition = messageLock.newCondition();
//
//        messageLock.lock();
//        try {
//            consumerSession.open((key, value) -> {
//                String keyString = byteArrayToString(key);
//                kafkaMessageHolder[0] = new KafkaMessage(keyString, new String(value));
//                messageLock.lock();
//                try {
//                    messageReceivedCondition.signalAll();
//                } finally {
//                    messageLock.unlock();
//                }
//            });
//
//            messageReceivedCondition.await();
//            consumerSession.close();
//        } finally {
//            messageLock.unlock();
//        }
//
//        return kafkaMessageHolder[0];
//    }

    private String byteArrayToString(byte[] data) {
        return data == null ? null : new String(data);
    }

    private Config buildTopologyConfig() {
        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(2);
        return config;
    }


    // @Test(groups = "integration", testName = "HBase test")
    public void hbaseTest() throws IOException, InterruptedException, TimeoutException {
        logger.info("---===HBASE TEST===---");
        String tableName = "window_tuples_t1";
        Configuration conf = HBaseConfiguration.create();

        conf.set(Const.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "hbasehost");
        conf.setInt(Const.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, 2181);

        HBaseAdmin admin = new HBaseAdmin(conf);
        boolean tableExist = admin.tableExists(tableName);
        logger.info("Table isExist: {}", tableExist);
        if (!tableExist) {
            HTableDescriptor td = new HTableDescriptor(TableName.valueOf("window_tuples_t1"));
            HColumnDescriptor cf1 = new HColumnDescriptor("cf");
            HColumnDescriptor cf2 = new HColumnDescriptor("somedata");
            td.addFamily(cf1);
            td.addFamily(cf2);
            admin.createTable(td);
            logger.info("TABLE CREATED");
        }

        String testString = "AbcDba 123$#_=-";
        HTable hTable = new HTable(conf, tableName);
        Put put = new Put("key-row-1".getBytes());
        put.add("somedata".getBytes(), "data1".getBytes(), testString.getBytes());
        logger.info("HBASE PUT");

        hTable.put(put);
        hTable.flushCommits();

        Get get = new Get("key-row-1".getBytes());
        Result res = hTable.get(get);
        byte[] bytes = res.getValue("somedata".getBytes(), "data1".getBytes());
        hTable.close();
        String resString = new String(bytes);
        logger.info(resString);
        assertEquals(testString, resString);

        String historyName = "log-history";
        boolean historyExist = admin.tableExists(historyName);
        if (!historyExist) {
            logger.info("TABLE {} NOT EXIST", historyName);
            HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(historyName));
            HColumnDescriptor cf1 = new HColumnDescriptor("metrics");
            htd.addFamily(cf1);
            admin.createTable(htd);
            logger.info("TABLE {} CREATED", historyName);
        }
    }

    @Test(testName = "Kafka test")
    public void kafkaTest() throws IOException, InterruptedException, TimeoutException {
        logger.info("---===Kafka TEST===---");

        KafkaAdmin kafkaAdmin = new KafkaAdmin(zk);
        String[] topics = {logTopic, alertTopic, testTopic};
        for (String topic : topics) {
            if (!kafkaAdmin.topicExists(topic)) {
                kafkaAdmin.createTopic(topic);
                logger.info("Created topic {}", topic);
            } else {
                logger.info("Topic {} exist!", topic);
            }
        }
        //kafkaAdmin.close();
        String json = new DataGenerator().getJson(1);

        KafkaProducerBuilder kpb = new KafkaProducerBuilder(kafkaBrokers);
        kpb.send(testTopic, json);
        kpb.close();
        logger.info("MESSAGE SENDED!!!");

        KafkaConsumerBuilder kcb = new KafkaConsumerBuilder(zk, testTopic);
        String message = kcb.receiveMessage();
        logger.info("MESSAGE RECEIVED!!!");

        assertEquals(json, message);
    }

    //@Test(groups = "integration", testName = "Topology test", priority = 1)
    public void testTopology() throws IOException, InterruptedException, TimeoutException {


        //KafkaProducer<String, String> producer = getKafkaProdBuilder().withType(KafkaProducer.TYPE_SYNC).build(String.class, String.class);

        TridentTopology topology = new TridentTopologyBuilder();
        Config config = buildTopologyConfig();

        LocalCluster cluster = new LocalCluster();
        logger.info("Start local cluster");
        cluster.submitTopology(topologyName, config, topology.build());
        //Thread.sleep(3000);

        List<LogMessage> testList = new ArrayList<>();
        testList.add(new LogMessage(1000l, "host1", "ERROR", "text1"));
        testList.add(new LogMessage(1100l, "host1", "ERROR", "text2"));
        testList.add(new LogMessage(1110l, "host1", "ERROR", "text2"));
        testList.add(new LogMessage(1200l, "host1", "ERROR", "text3"));
        testList.add(new LogMessage(1000l, "host1", "ERROR", "text4"));
        testList.add(new LogMessage(5000l, "host1", "ERROR", "text4"));
        testList.add(new LogMessage(6500l, "host1", "ERROR", "text4"));
        testList.add(new LogMessage(7700l, "host1", "ERROR", "text4"));
        testList.add(new LogMessage(1040l, "host2", "ERROR", "text5"));
//        testList.add(new LogMessage(10400l, "host222", "ERROR", "text5"));
//        testList.add(new LogMessage(16410l, "host222", "ERROR", "text5"));
        testList.add(new LogMessage(1020l, "host2", "ERROR", "text6"));
        testList.add(new LogMessage(1030l, "host2", "INFO", "text6"));

        String json_1 = Utils.objectToJson(testList);

        KafkaMessage km = null;
        //producer.sendMessage(logTopic, "json-log", json_1);
        //km = receiveKafkaMessage(alertTopic);
        List<LogRate> rates = Utils.jsonToRateList(km.getValue());
        assertNotNull(rates);
        assertEquals(rates.size(), 2);
        for (LogRate rate : rates) {

            if ("host1".equals(rate.getValue())) {
                assertEquals(5d, rate.getRate());
            }

            if ("host2".equals(rate.getValue())) {
                assertEquals(3d, rate.getRate());
            }
        }
        logger.info("ALERT RECEIVED!!!");
        logger.info(km.toString());

        logger.info("Shutdown local cluster");
        cluster.shutdown();

        String historyName = "log-history";
        Configuration conf = HBaseConfiguration.create();
        HTable hTable = new HTable(conf, historyName);

        ResultScanner rs = hTable.getScanner("metrics".getBytes());
        for (Iterator<Result> it = rs.iterator(); it.hasNext(); ) {
            Result r = it.next();
            logger.info(new String(r.getValue("metrics".getBytes(), "count".getBytes())));
            logger.info(new String(r.getValue("metrics".getBytes(), "rate".getBytes())));
        }

        hTable.close();

    }
}
