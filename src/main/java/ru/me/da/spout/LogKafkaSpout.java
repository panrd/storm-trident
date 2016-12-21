package ru.me.da.spout;

import ru.me.da.schema.LogScheme;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;

/**
 * Created by Pavel Popov on 21.11.2016.
 */
public class LogKafkaSpout {

    public static OpaqueTridentKafkaSpout newSpout(String host, String topic, String inTupleField) {
        BrokerHosts zk = new ZkHosts(host);
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, topic);
        spoutConf.scheme = new LogScheme(inTupleField);
        OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
        return spout;
    }

}
