package ru.me.da.builder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.hbase.trident.windowing.HBaseWindowsStoreFactory;
import org.apache.storm.kafka.trident.TridentKafkaStateFactory;
import org.apache.storm.kafka.trident.TridentKafkaUpdater;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.me.da.factory.LogKafkaProduceFactory;
import ru.me.da.proc.*;
import ru.me.da.spout.LogKafkaSpout;
import ru.me.da.util.Const;
import ru.me.da.util.StreamField;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by Pavel Popov on 02.12.2016.
 */
public class TridentTopologyBuilder extends TridentTopology {

    private static Logger logger = LoggerFactory.getLogger(TridentTopologyBuilder.class);

    private static String zk = "hbasehost:2185";

    public TridentTopologyBuilder() {
        super();
    }

    private void buildTopology() {
        try {
            Configuration conf = HBaseConfiguration.create();
            Stream stream = this.newStream("trident-stream", LogKafkaSpout.newSpout(zk, "log-topic", StreamField.SPOUT_IN_TUPLE_STR));

            BaseWindowedBolt.Duration duration = new BaseWindowedBolt.Duration(1, TimeUnit.SECONDS);
            BaseWindowedBolt.Duration interval = new BaseWindowedBolt.Duration(1, TimeUnit.SECONDS);

            //Хранение всех tuples в 1 окне для последующей обработки на узлах топологии
            Map<String, Object> hconf = new HashMap<>();
            hconf.put(Const.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "hbasehost");
            hconf.put(Const.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, 2181);
            WindowsStoreFactory wsf = new HBaseWindowsStoreFactory(hconf, "window_tuples_t1", "cf".getBytes("UTF-8"), "tuples".getBytes("UTF-8"));

            //Конфиг Kafka alert топика
            TridentKafkaStateFactory stateFactory = new LogKafkaProduceFactory().build();
            stream.parallelismHint(2)
                    .each(StreamField.SPOUT_IN_TUPLE, new KafkaLogConverter(), StreamField.LOG_OBJECTS)
                    .slidingWindow(
                            duration,
                            interval,
                            wsf,
                            StreamField.LOG_OBJECTS,
                            new LogAggregator(),
                            StreamField.RAW_LOG_OBJECTS)
                    .aggregate(
                            StreamField.RAW_LOG_OBJECTS,
                            new LogEventCounter(conf),
                            StreamField.LOG_RATE)
                    .each(StreamField.LOG_RATE,
                            new LevelExtractor(Const.ERROR, 1),
                            StreamField.of(StreamField.FILTERED_LEVEL, StreamField.FILTERED_RATE))
                    .each(
                            StreamField.of(StreamField.FILTERED_LEVEL, StreamField.FILTERED_RATE),
                            new LogRateToJSON(),
                            StreamField.of(StreamField.LEVEL, StreamField.ERROR_JSON))
                    .partitionPersist(
                            stateFactory,
                            StreamField.of(StreamField.LEVEL, StreamField.ERROR_JSON),
                            new TridentKafkaUpdater());

        } catch (Exception ex) {
            logger.error(ex.getLocalizedMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public StormTopology build() {
        buildTopology();
        return super.build();
    }
}
