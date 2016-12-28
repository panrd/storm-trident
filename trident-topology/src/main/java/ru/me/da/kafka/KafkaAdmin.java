package ru.me.da.kafka;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

import java.util.Properties;

/**
 * Created by Pavel Popov on 21.12.2016.
 */
public class KafkaAdmin {

    private ZkUtils zkUtils;

    public KafkaAdmin(String zk) {
        this.zkUtils = ZkUtils.apply(zk, 5000, 5000, false);

    }

    public void createTopic(String name) {
        AdminUtils.createTopic(zkUtils, name, 1, 1, new Properties(), null);
    }

    public boolean topicExists(String name) {
        return AdminUtils.topicExists(zkUtils, name);
    }

    public void close() {
        zkUtils.close();
    }

}
