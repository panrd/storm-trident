package ru.me.da.serializer;

import java.io.Serializable;

/**
 * Created by Pavel Popov on 10.01.2017.
 */
public interface ITopologySerialization {

    <T> T fromJson(String json, Class<T> clazz);

    String toJson(Object obj);

}
