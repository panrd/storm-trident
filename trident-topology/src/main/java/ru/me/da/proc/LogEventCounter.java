package ru.me.da.proc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.me.da.model.LogMessage;
import ru.me.da.model.LogRate;
import ru.me.da.util.Const;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Pavel Popov on 30.11.2016.
 */
public class LogEventCounter extends BaseAggregator<LogEventCounter.LogEventState> {

    private static Logger logger = LoggerFactory.getLogger(LogEventCounter.class);

    private ThreadLocal<HTable> tableThreadLocal;
    private Queue<HTable> hTables = new ConcurrentLinkedQueue<>();

    public LogEventCounter(Configuration conf) {
        logger.info("INIT LOGCOUNTER");
    }

    private class Frequency {
        long number = 0;
        long prevTS = 0;
        long count = 0;
        boolean overlap = false;

        @Override
        public String toString() {
            return "Frequency{" +
                    "number=" + number +
                    ", count=" + count +
                    '}';
        }
    }

    static class LogEventState {
        List<LogMessage> logCollection = new ArrayList<>();
    }

    @Override
    public LogEventState init(Object batchId, TridentCollector collector) {
        if (tableThreadLocal == null) {
            Configuration conf = HBaseConfiguration.create();
            conf.set(Const.HBASE_CONFIGURATION_ZOOKEEPER_QUORUM, "hbasehost");
            conf.setInt(Const.HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT, 2181);
            tableThreadLocal = new ThreadLocal<HTable>() {
                @Override
                protected HTable initialValue() {
                    try {
                        HTable table = new HTable(conf, "log-history");

                        return table;
                    } catch (IOException ioex) {
                        throw new RuntimeException("Что-то случилось при создании таблицы...");
                    }
                }
            };
        }
        return new LogEventState();
    }

    @Override
    public void aggregate(LogEventState val, TridentTuple tuple, TridentCollector collector) {
        try {
            if (!tuple.isEmpty()) {
                List<LogMessage> partitionList = (List<LogMessage>) tuple.get(0);
                val.logCollection.addAll(partitionList);
            }
        } catch (Exception ex) {
            logger.error("STACKTRACE:{0}", ex);
        }
    }

    @Override
    public void complete(LogEventState val, TridentCollector collector) {
        HTable table = tableThreadLocal.get();
        List<Put> puts = new ArrayList<>();
        long start = System.nanoTime();
        List<LogMessage> messages = val.logCollection;
        Map<String, LogRate> levelsCount = new HashMap<>();
        Map<String, Frequency> frequencyMap = null;
        if (!messages.isEmpty()) {
            frequencyMap = new HashMap<>();
            Map<String, Frequency> finalFrequencyMap = frequencyMap;
            messages.forEach(m -> {
                String level = m.getLevel();
                String host = m.getHost();

                LogRate levelRate = incrementCount(levelsCount, Const.LEVEL, level);
                Map<String, LogRate> hostMap = levelRate.getSubrates();
                if (hostMap == null) {
                    hostMap = new HashMap<>();
                    levelRate.setSubrates(hostMap);
                }

                incrementCount(hostMap, Const.HOST, host);

                String l = level;
                String lh = level + "#" + host;
                speedCount(finalFrequencyMap, m, l);
                speedCount(finalFrequencyMap, m, lh);
            });
        }

        Map<String, LogRate> result = new HashMap<>();

        if (!levelsCount.isEmpty()) {
            Map<String, Frequency> finalFrequencyMap = frequencyMap;
            levelsCount.forEach((k, v) -> {
                double rateValue = calcRate(finalFrequencyMap, k);
                LogRate rate = new LogRate(v.getType(), v.getValue(), v.getCount(), rateValue, null);
                if (v.getSubrates() != null) {
                    Map<String, LogRate> subrates = new HashMap<>();
                    v.getSubrates().forEach((s_k, s_v) -> {
                        String fKey = k + "#" + s_k;
                        double s_rateValue = calcRate(finalFrequencyMap, fKey);
                        LogRate s_rate = new LogRate(s_v.getType(), s_v.getValue(), s_v.getCount(), s_rateValue, null);
                        result.put(fKey, s_rate);
                        puts.add(storeLogInHBase(table, k, s_k, s_v.getCount(), s_rateValue));
                    });
                    rate.setSubrates(subrates);
                }
                result.put(k, rate);
            });
        }
        if (!result.isEmpty()) {
            try {
                if (!puts.isEmpty()) {
                    table.put(puts);
                    logger.error("SUCCEED STORE {} MESSAGES", puts.size());
                }
            } catch (IOException ioex) {
                logger.error(ioex.getLocalizedMessage(), ioex);
            }
            long end = System.nanoTime();
            logger.info(Math.round((double) (end - start) / 1000000d) + " ms");
            double approxRate = 1000000000d / (double) (end - start);
            logger.info("Approx throughput:" + Math.round(approxRate * messages.size()) + " msg/sec");
            logger.info("Messages proceed number: {}", messages.size());
        }
        collector.emit(new Values(result));
    }

    private double calcRate(Map<String, Frequency> fMap, String fKey) {
        if (fMap == null || fKey == null || "".equals(fKey)) {
            return 0;
        }
        Frequency f = fMap.get(fKey);
        if (f == null) {
            return 0;
        }
        return (double) f.count / (double) f.number;
    }


    private LogRate incrementCount(Map<String, LogRate> map, String rateType, String value) {
        LogRate rate = map.get(value);
        if (rate == null) {
            rate = new LogRate(rateType, value, 1, 0, null);
            map.put(value, rate);
        } else {
            int count = rate.getCount() + 1;
            rate.setCount(count);
        }
        return rate;
    }

    private void speedCount(Map<String, Frequency> frequencyMap, LogMessage msg, String byField) {
        Frequency f = frequencyMap.get(byField);
        if (f == null) {
            f = new Frequency();
            f.prevTS = msg.getTimestamp();
            f.count = 0;
            f.number = 0;
            frequencyMap.put(byField, f);
        } else {
            long diff = msg.getTimestamp() - f.prevTS;
            if (diff < 1000) {
                if (!f.overlap) {
                    f.number++;
                    if (f.count == 0) {
                        f.count = 1;
                    } else {
                        f.count++;
                    }
                }
                f.overlap = true;
                f.count++;
            } else {
                f.overlap = false;
            }
            f.prevTS = msg.getTimestamp();
        }
    }

    private Put storeLogInHBase(HTable table, String level, String host, Integer count, Double rate) {
        Put put = null;
        if (level != null && host != null && count != null && rate != null) {
            try {
                put = new Put(buildRowKey(level, host));
                put.add("metrics".getBytes(), "count".getBytes(), count.toString().getBytes());
                put.add("metrics".getBytes(), "rate".getBytes(), rate.toString().getBytes());
                //logger.error("SUCCEED STORE: {}", level + "#" + host);
            } catch (Exception ioex) {
                logger.error(ioex.getLocalizedMessage(), ioex);
            }
        }
        return put;
    }

    private byte[] buildRowKey(String level, String host) {
        long ts = new Date().getTime();
        StringBuilder sb = new StringBuilder();
        sb.append(level);
        sb.append("#");
        sb.append(host);
        sb.append("#");
        sb.append(Long.MAX_VALUE - ts);
        return sb.toString().getBytes();
    }

    @Override
    public void cleanup() {
        logger.info("COUNTER STOPPING...");
        if (hTables != null && !hTables.isEmpty()) {
            for (HTable t : hTables) {
                try {
                    t.close();
                } catch (IOException ioex) {
                    logger.error(ioex.getLocalizedMessage(), ioex);
                }
            }
        }
    }

}
