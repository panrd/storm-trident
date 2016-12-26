package ru.me.da.util;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import ru.me.da.model.LogMessage;
import ru.me.da.model.LogRate;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Pavel Popov on 21.11.2016.
 */
public class Utils {

    private static Gson _gson;

    static {
        _gson = new Gson();
    }

    public static LogMessage jsonToLog(String json) {
        LogMessage msg = null;
        try {
            msg = _gson.fromJson(json, LogMessage.class);
        } catch (JsonSyntaxException ex) {
            //nop
        }
        return msg;
    }

    public static String objectToJson(Object obj) {
        return _gson.toJson(obj);
    }

    public static List<LogMessage> jsonToLogList(String json) {
        List<LogMessage> list = null;
        try {
            list = Arrays.asList(_gson.fromJson(json, LogMessage[].class));
            return list.stream().collect(Collectors.toList());
        } catch (JsonSyntaxException ex) {
            //nop
        }
        return list;
    }

    public static List<LogRate> jsonToRateList(String json) {
        List<LogRate> list = null;
        try {
            list = Arrays.asList(_gson.fromJson(json, LogRate[].class));
            return list.stream().collect(Collectors.toList());
        } catch (JsonSyntaxException ex) {
            //nop
        }
        return list;
    }

//    public static <T> List<T> extractTupleValues(ITuple tuple, Class<T> clazz) {
//        List<T> list = new ArrayList<>();
//        if (tuple != null && tuple.size() > 0) {
//            list.addAll((List<T>) tuple.getValue(0));
//        }
//        return list;
//    }

}
