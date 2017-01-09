package ru.me.da;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;
import ru.me.da.model.LogMessage;
import ru.me.da.util.Utils;

import static org.testng.Assert.assertEquals;

/**
 * Created by Pavel Popov on 06.12.2016.
 */
public class UtilsTest {

    private static final Logger logger = LoggerFactory.getLogger(UtilsTest.class);

    private String level = "ERROR";
    private String host = "hOSt";
    private String text = "tExt";
    private long timestamp = 130000000000l;
    private String json = "{'level':'ERROR','host':'hOSt','text':'tExt','timestamp':130000000000}";

    @Test
    public void bidirectionalSerDesTest() {
        logger.info("---===Utils test===---");

        LogMessage m1 = new LogMessage();
        m1.setLevel(level);
        m1.setText(text);
        m1.setHost(host);
        m1.setTimestamp(timestamp);

        LogMessage m2 = Utils.jsonToLog(json);

        assertEquals(m1, m2);

        String testJson = Utils.objectToJson(m2);
        LogMessage m3 = Utils.jsonToLog(testJson);

        assertEquals(m1, m3);
    }
}
