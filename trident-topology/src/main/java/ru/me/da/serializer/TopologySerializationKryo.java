package ru.me.da.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.tools.ant.filters.StringInputStream;
import ru.me.da.model.LogMessage;
import ru.me.da.model.LogRate;

import java.io.ByteArrayOutputStream;
import java.io.Serializable;

/**
 * Created by Pavel Popov on 10.01.2017.
 */
public class TopologySerializationKryo implements ITopologySerialization {

    private Kryo kryo;

    public TopologySerializationKryo() {
        kryo = new Kryo();

        kryo.register(LogRate.class);
        kryo.register(LogMessage.class);

    }

    @Override
    public <T> T fromJson(String json, Class<T> clazz) {
        try (Input input = new Input(new StringInputStream(json))) {
            T t = kryo.readObject(input, clazz);
            return t;
        }
    }

    @Override
    public String toJson(Object obj) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (Output output = new Output(baos)) {
            kryo.writeObject(output, obj);
        }
        return null;
    }
}
