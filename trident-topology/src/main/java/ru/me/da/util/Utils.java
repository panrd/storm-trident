package ru.me.da.util;

import com.google.gson.JsonSyntaxException;
import ru.me.da.model.LogMessage;
import ru.me.da.model.LogRate;
import ru.me.da.serializer.ITopologySerialization;
import ru.me.da.serializer.TopologySerializationGson;
import ru.me.da.serializer.TopologySerializationKryo;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Pavel Popov on 21.11.2016.
 */
public class Utils {


    private static ITopologySerialization serializator;

    public static LogMessage jsonToLog(String json) {
        LogMessage msg = null;
        try {
            serializator = getSerializator("kryo");
            msg = serializator.fromJson(json, LogMessage.class);
        } catch (JsonSyntaxException ex) {
            //nop
        }
        return msg;
    }

    public static String objectToJson(Object obj) {
        serializator = getSerializator("kryo");
        return serializator.toJson(obj);
    }

    public static List<LogMessage> jsonToLogList(String json) {
        List<LogMessage> list = null;
        try {
            serializator = getSerializator("kryo");
            list = Arrays.asList(serializator.fromJson(json, LogMessage[].class));
            return list.stream().collect(Collectors.toList());
        } catch (JsonSyntaxException ex) {
            //nop
        }
        return list;
    }

    public static List<LogRate> jsonToRateList(String json) {
        List<LogRate> list = null;
        try {
            serializator = getSerializator("kryo");
            list = Arrays.asList(serializator.fromJson(json, LogRate[].class));
            return list.stream().collect(Collectors.toList());
        } catch (JsonSyntaxException ex) {
            //nop
        }
        return list;
    }

    public static ITopologySerialization getSerializator(String type) {
        if ("kryo".equalsIgnoreCase(type)) {
            return new TopologySerializationKryo();
        }
        if ("gson".equalsIgnoreCase(type)) {
            return new TopologySerializationGson();
        }
        throw new RuntimeException("Unknown serialization");
    }

//    public static <T> List<T> extractTupleValues(ITuple tuple, Class<T> clazz) {
//        List<T> list = new ArrayList<>();
//        if (tuple != null && tuple.size() > 0) {
//            list.addAll((List<T>) tuple.getValue(0));
//        }
//        return list;
//    }

}
