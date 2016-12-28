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
import ru.me.da.util.DataGenerator;
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

    private String byteArrayToString(byte[] data) {
        return data == null ? null : new String(data);
    }

    private Config buildTopologyConfig() {
        Config config = new Config();
        config.setDebug(false);
        config.setNumWorkers(2);
        return config;
    }


    @Test(testName = "HBase test")
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
        kafkaAdmin.close();
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

    @Test(testName = "Topology test", priority = 1)
    public void testTopology() throws IOException, InterruptedException, TimeoutException {

        TridentTopology topology = new TridentTopologyBuilder();
        Config config = buildTopologyConfig();

        LocalCluster cluster = new LocalCluster();
        logger.info("Start local cluster");
        cluster.submitTopology(topologyName, config, topology.build());
        Thread.sleep(10000);
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

        String json_1 = Utils.objectToJson(testList);

        KafkaProducerBuilder kpb = new KafkaProducerBuilder(kafkaBrokers);
        kpb.send(logTopic, json_1);
        kpb.close();

        KafkaConsumerBuilder kcb = new KafkaConsumerBuilder(zk, alertTopic);
        String message = kcb.receiveMessage();
        logger.info("---RECEIVED---");
        logger.info(message);
        List<LogRate> rates = Utils.jsonToRateList(message);
        assertNotNull(rates);
        assertEquals(rates.size(), 1);
        for (LogRate rate : rates) {

            if ("host1".equals(rate.getValue())) {
                assertEquals(5d, rate.getRate());
            }

            if ("host2".equals(rate.getValue())) {
                assertEquals(3d, rate.getRate());
            }
        }
        logger.info("ALERT RECEIVED!!!");

        logger.info("Shutdown local cluster");
        cluster.shutdown();
    }
}
