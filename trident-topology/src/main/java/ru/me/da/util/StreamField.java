package ru.me.da.util;

import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

/**
 * Created by Pavel Popov on 25.11.2016.
 */
public class StreamField {

    public static String SPOUT_IN_TUPLE_STR = "inTupleField";
    public static Fields SPOUT_IN_TUPLE = _f(SPOUT_IN_TUPLE_STR);

    public static String LOG_OBJECTS_STR = "logObjects";
    public static Fields LOG_OBJECTS = _f(LOG_OBJECTS_STR);

    public static String ERROR_STR = Const.ERROR;
    public static Fields ERROR = _f(ERROR_STR);

    public static String ERROR_JSON_STR = "errorJson";
    public static Fields ERROR_JSON = _f(ERROR_JSON_STR);

    public static String LEVEL_STR = "level";
    public static Fields LEVEL = _f(LEVEL_STR);

    public static String FILTERED_LEVEL_STR = "filteredLevel";
    public static Fields FILTERED_LEVEL = _f(FILTERED_LEVEL_STR);

    public static String FILTERED_RATE_STR = "filteredRate";
    public static Fields FILTERED_RATE = _f(FILTERED_RATE_STR);

    public static String RAW_LOG_OBJECTS_STR = "rawLogObjects";
    public static Fields RAW_LOG_OBJECTS = _f(RAW_LOG_OBJECTS_STR);

    public static String LOG_FREQUENCY_STR = "logFrequency";
    public static Fields LOG_FREQUENCY = _f(LOG_FREQUENCY_STR);

    public static String LOG_COUNT_STR = "logCount";
    public static Fields LOG_COUNT = _f(LOG_COUNT_STR);

    public static String LOG_RATE_STR = "logRate";
    public static Fields LOG_RATE = _f(LOG_RATE_STR);

    private static Fields _f(String name) {
        return new Fields(name);
    }

    public static Fields of(Fields... fields) {
        if (fields == null || fields.length == 0) {
            return new Fields();
        }
        List<String> fList = new ArrayList<>();
        Stream.of(fields).forEach(f -> fList.addAll(f.toList()));
        return new Fields(fList);
    }

}
