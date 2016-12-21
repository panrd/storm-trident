package ru.me.da.proc;

import ru.me.da.model.LogMessage;
import ru.me.da.util.Utils;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * Created by Pavel Popov on 29.11.2016.
 *
 * Конвертация json-содержимого из кафки в java-объект
 */
public class KafkaLogConverter extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        if (!tuple.isEmpty()) {
            String json = tuple.getString(0);
            if (json != null && !"".equals(json)) {
                //Потенциально - самое узкое место из-за использования Gson
                List<LogMessage> messageList = Utils.jsonToLogList(json);
                if (messageList != null && !messageList.isEmpty()) {
                    collector.emit(new Values(messageList));
                }
            }
        }
    }
}
