package ru.me.da.model;

import java.io.Serializable;

/**
 * Created by Pavel Popov on 18.11.2016.
 */
public class LogMessage {
    private Long timestamp;
    private String host;
    private String level;
    private String text;

    public LogMessage() {
    }

    public LogMessage(Long timestamp, String host, String level, String text) {
        this.timestamp = timestamp;
        this.host = host;
        this.level = level;
        this.text = text;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LogMessage message = (LogMessage) o;

        if (timestamp != null ? !timestamp.equals(message.timestamp) : message.timestamp != null) return false;
        if (host != null ? !host.equals(message.host) : message.host != null) return false;
        if (level != null ? !level.equals(message.level) : message.level != null) return false;
        return text != null ? text.equals(message.text) : message.text == null;

    }
}
