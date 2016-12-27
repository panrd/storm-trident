package ru.me.da.util;

/**
 * Created by Pavel Popov on 18.11.2016.
 */
public class Const {
    public static String TRACE = "TRACE";
    public static String DEBUG = "DEBUG";
    public static String INFO = "INFO";
    public static String WARN = "WARN";
    public static String ERROR = "ERROR";

    public static String LEVEL = "level";
    public static String HOST = "host";

    public static final String HBASE_CONFIGURATION_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String HBASE_CONFIGURATION_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientPort";
    public static final String KEYVALUE_MAXSIZE = "hbase.client.keyvalue.maxsize";

    public static final int KEYVALUE_MAXSIZE_100M = 100 * 1024 * 1024;
    public static final int KEYVALUE_MAXSIZE_1G = 1000 * 1024 * 1024;
}
