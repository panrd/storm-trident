package ru.me.da.serializer;

import com.google.gson.Gson;

import java.io.Serializable;

/**
 * Created by Pavel Popov on 10.01.2017.
 */
public class TopologySerializationGson implements ITopologySerialization {

    private Gson _gson;

    public TopologySerializationGson() {
        _gson = new Gson();
    }

    @Override
    public <T> T fromJson(String json, Class<T> clazz) {
        return _gson.fromJson(json, clazz);
    }

    @Override
    public String toJson(Object obj) {
        return _gson.toJson(obj);
    }
}
