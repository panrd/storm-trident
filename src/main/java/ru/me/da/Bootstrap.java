package ru.me.da;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.trident.TridentTopology;
import ru.me.da.builder.TridentTopologyBuilder;

/**
 * Created by Pavel Popov on 18.11.2016.
 */
public class Bootstrap {

    private static String topologyName = "logger-topology";

    public static void main(String[] args) {
        try {

            TridentTopology topology = new TridentTopologyBuilder();

            Config config = new Config();

            config.setDebug(false);
            config.setNumWorkers(2);

            StormSubmitter.submitTopology(topologyName, config, topology.build());

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
