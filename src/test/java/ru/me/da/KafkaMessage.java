package ru.me.da;

/**
 * Created by Pavel Popov on 06.12.2016.
 */
public class KafkaMessage {

    private String key;
    private String value;

    public KafkaMessage() {
    }

    public KafkaMessage(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "KafkaMessage{" +
                "key='" + key + '\'' +
                ", value='" + value + '\'' +
                '}';
    }
}
