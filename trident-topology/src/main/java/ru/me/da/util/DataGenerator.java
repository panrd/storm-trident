package ru.me.da.util;

import com.google.gson.Gson;
import ru.me.da.model.LogMessage;
import ru.me.da.util.Const;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

/**
 * Created by Pavel Popov on 06.12.2016.
 */
public class DataGenerator {
    private static String LOG_TOPIC;
    private static String[] levels;
    private static String[] hosts;
    private static String[] texts;
    private static Random rnd;
    private static Gson gson;

    static {
        LOG_TOPIC = "log-topic";
        levels = new String[]{
                Const.DEBUG,
                Const.ERROR,
                Const.INFO,
                Const.TRACE,
                Const.WARN
        };

        hosts = new String[]{
                "http://host01",
                "http://host02",
                "http://host03",
                "http://host04",
                "http://host05",
                "http://host06",
                "http://host07",
                "http://host08",
                "http://host09",
                "http://host10"
        };

        texts = new String[]{
                "Kafka Streams is a Java library for building distributed stream processing apps using Apache Kafka",
                "So what did we learn? Lots. One of the key misconceptions we had was that stream processing would be used in a way sort of like a real-time MapReduce layer",
                "Using the Kafka APIs directly works well for simple things.",
                "The first aspect of how Kafka Streams makes building streaming services simpler is that it is cluster and framework free—it is just a library.",
                "The next key way Kafka Streams simplifies streaming applications is that it fully integrates the concepts of tables and streams."
        };
        rnd = new Random();
        gson = new Gson();
    }

    public String getJson(int length) {
        if (length == 0) {
            length = 1;
        }
        List<LogMessage> messageList = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            messageList.add(generateMessage());
        }

        String msg2Topic = gson.toJson(messageList);
        return msg2Topic;
    }

    private static LogMessage generateMessage() {
        LogMessage msg = new LogMessage();
        msg.setLevel(levels[rnd.nextInt(levels.length)]);
        msg.setHost(hosts[rnd.nextInt(hosts.length)]);

        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.MINUTE, rnd.nextInt(50));
        cal.set(Calendar.SECOND, rnd.nextInt(50));
        cal.set(Calendar.MILLISECOND, 0);

        msg.setTimestamp(cal.getTimeInMillis());
        msg.setText(texts[rnd.nextInt(texts.length)]);

        return msg;
    }
}
