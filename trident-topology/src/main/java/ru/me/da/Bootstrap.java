package ru.me.da;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.trident.TridentTopology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.me.da.builder.TridentTopologyBuilder;

/**
 * Created by Pavel Popov on 18.11.2016.
 */
public class Bootstrap {

    private static Logger logger = LoggerFactory.getLogger(Bootstrap.class);

    private static String topologyName = "logger-topology";

    public static void main(String[] args) {
        try {

            TridentTopology topology = new TridentTopologyBuilder();

            LocalCluster cluster = new LocalCluster();
            logger.info("Start local cluster");

            Config config = new Config();
            config.setDebug(false);
            config.setNumWorkers(2);

            cluster.submitTopology(topologyName, config, topology.build());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
