package ru.me.da.util;

import com.google.gson.Gson;
import ru.me.da.model.LogMessage;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

/**
 * Created by Pavel Popov on 06.12.2016.
 */
public class DataGenerator {
    private static String[] levels;
    private static String[] hosts;
    private static String[] texts;
    private static Random rnd;
    private static Gson gson;

    static {
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
                "Message #1",
                "Message #2",
                "Message #3",
                "Message #4",
                "Message #5",
                "Message #6",
                "Message #7",
                "Message #8",
                "Message #9",
                "Message #10"
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
        cal.set(Calendar.MINUTE, 1);
        cal.set(Calendar.SECOND, 1);
        cal.set(Calendar.MILLISECOND, 111);

        msg.setTimestamp(cal.getTimeInMillis());
        msg.setText(texts[rnd.nextInt(texts.length)]);

        return msg;
    }
}
