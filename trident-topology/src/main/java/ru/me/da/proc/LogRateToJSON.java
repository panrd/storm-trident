package ru.me.da.proc;

import ru.me.da.model.LogRate;
import ru.me.da.util.Utils;
import ru.me.da.util.StreamField;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * Created by Pavel Popov on 25.11.2016.
 */
public class LogRateToJSON extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        if (!tuple.isEmpty()) {
            String level = tuple.getStringByField(StreamField.FILTERED_LEVEL_STR);
            List<LogRate> logList = (List) tuple.getValueByField(StreamField.FILTERED_RATE_STR);
            String json = Utils.objectToJson(logList);
            collector.emit(new Values(level, json));
        }
    }
}
