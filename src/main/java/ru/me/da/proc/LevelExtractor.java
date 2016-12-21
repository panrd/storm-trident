package ru.me.da.proc;

import ru.me.da.model.LogRate;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by Pavel Popov on 01.12.2016.
 */
public class LevelExtractor extends BaseFunction {

    private String level;
    private double threshold = 0;

    public LevelExtractor(String level, double threshold) {
        this.level = level;
        this.threshold = threshold;
    }

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        if (!tuple.isEmpty() && level != null && !"".equals(level)) {
            Map<String, LogRate> map = (Map) tuple.get(0);
            if (map != null && !map.isEmpty()) {
                List<LogRate> filteredList = map.entrySet().stream().filter(es -> !es.getKey().equals(level) && es.getKey().startsWith(level) && es.getValue().getRate() > threshold).map(es -> es.getValue()).collect(Collectors.toList());
                collector.emit(new Values(level, filteredList));
            }
        } else {
            collector.emit(new Values(null, null));
        }
    }
}
