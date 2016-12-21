package ru.me.da.kafka;

import kafka.admin.AdminUtils;
import org.I0Itec.zkclient.ZkClient;

import java.util.Properties;

/**
 * Created by Pavel Popov on 21.12.2016.
 */
public class KafkaAdmin {

    private String zk;
    private ZkClient zkClient;

    public KafkaAdmin(String zk) {
        this.zk = zk;
        this.zkClient = new ZkClient(zk, 5000, 5000);
    }

    public void createTopic(String name) {
        AdminUtils.createTopic(zkClient, name, 1, 1, new Properties());
    }

    public boolean topicExists(String name) {
        return AdminUtils.topicExists(zkClient, name);
    }

}
