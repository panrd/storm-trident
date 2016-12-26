package ru.me.da.kafka;

import kafka.admin.AdminUtils;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;

import java.util.Properties;

/**
 * Created by Pavel Popov on 21.12.2016.
 */
public class KafkaAdmin {

    private ZkClient zkClient;

    public KafkaAdmin(String zk) {
        this.zkClient = new ZkClient(zk, 5000, 5000, ZKStringSerializer$.MODULE$);
    }

    public void createTopic(String name) {
        AdminUtils.createTopic(zkClient, name, 1, 1, new Properties());
    }

    public boolean topicExists(String name) {
        return AdminUtils.topicExists(zkClient, name);
    }

    public void close() {
        zkClient.close();
    }

}
