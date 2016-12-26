package ru.me.da.kafka;

/**
 * Created by Pavel Popov on 22.12.2016.
 */
public interface MessageHandler {

    void onMessage(byte[] value);

}
