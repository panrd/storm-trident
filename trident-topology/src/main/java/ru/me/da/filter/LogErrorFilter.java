package ru.me.da.filter;

import ru.me.da.model.LogRate;
import ru.me.da.util.Const;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;

/**
 * Created by Pavel Popov on 24.11.2016.
 */
public class LogErrorFilter extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {
        if (!tuple.isEmpty()) {
            Object o = tuple.get(0);
            if (o instanceof List) {
                Map<String, LogRate> rateMap = (Map<String, LogRate>) o;
                if (rateMap == null) {
                    return false;
                }
                LogRate errorRate = rateMap.get(Const.ERROR);
                if (errorRate == null) {
                    return false;

                }
                return new Double(1d).compareTo(errorRate.getRate()) == -1;
            }
        }
        return false;
    }
}
