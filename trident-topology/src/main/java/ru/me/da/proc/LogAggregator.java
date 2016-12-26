package ru.me.da.proc;

import org.apache.storm.trident.operation.BaseAggregator;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.me.da.model.LogMessage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Pavel Popov on 23.11.2016.
 * <p>
 * Глобальная агрегация по всем partition's
 */
public class LogAggregator extends BaseAggregator<LogAggregator.State> {

    private static Logger logger = LoggerFactory.getLogger(LogAggregator.class);

    static class State {
        List<LogMessage> logCollection = new ArrayList<>(10000);
    }

    @Override
    public State init(Object batchId, TridentCollector collector) {
        return new State();
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
    }

    @Override
    public void aggregate(State val, TridentTuple tuple, TridentCollector collector) {
        try {
            logger.info("log aggregate");
            if (!tuple.isEmpty()) {
                List<LogMessage> partitionList = (List<LogMessage>) tuple.get(0);
                val.logCollection.addAll(partitionList);
            }
        } catch (Exception ex) {
            logger.error("STACKTRACE:{0}", ex);
        }
    }

    @Override
    public void complete(State val, TridentCollector collector) {
        val.logCollection = val.logCollection.stream().sorted((m1, m2) -> m1.getTimestamp().compareTo(m2.getTimestamp())).collect(Collectors.toList());
        collector.emit(new Values(val.logCollection));
    }
}
