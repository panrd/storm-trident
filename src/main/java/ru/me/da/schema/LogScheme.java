package ru.me.da.schema;

import org.apache.storm.spout.MultiScheme;
import org.apache.storm.tuple.Fields;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hbase.HConstants.UTF8_CHARSET;

/**
 * Created by Pavel Popov on 22.11.2016.
 */
public class LogScheme implements MultiScheme {

    private String inTupleField;

    public LogScheme(String inTupleField) {
        this.inTupleField = inTupleField;
    }

    @Override
    public Iterable<List<Object>> deserialize(ByteBuffer ser) {
        String str;
        List<List<Object>> it;
        if (ser.hasArray()) {
            int base = ser.arrayOffset();
            str = new String(ser.array(), base + ser.position(), ser.remaining());
        } else {
            byte[] ret = new byte[ser.remaining()];
            ser.get(ret, 0, ret.length);
            str = new String(ret, UTF8_CHARSET);
        }

        if ("".equals(str)) {
            return null;
        }
        it = new ArrayList(1);
        List<Object> stringList = new ArrayList<>(1);
        stringList.add(str);
        it.add(stringList);
        return it;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields(inTupleField);
    }
}
