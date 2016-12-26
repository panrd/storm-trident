package ru.me.da.model;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by Pavel Popov on 23.11.2016.
 */
public class LogRate implements Serializable {

    /**
     * Тип метрики: {@value "host" или "level"}
     */
    private String type;
    private String value;
    private int count;
    private double rate;
    private Map<String, LogRate> subrates;

    public LogRate() {
    }

    public LogRate(String type, String value, int count, double rate, Map<String, LogRate> subrates) {
        this.type = type;
        this.value = value;
        this.count = count;
        this.rate = rate;
        this.subrates = subrates;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public double getRate() {
        return rate;
    }

    public void setRate(double rate) {
        this.rate = rate;
    }

    public Map<String, LogRate> getSubrates() {
        return subrates;
    }

    public void setSubrates(Map<String, LogRate> subrates) {
        this.subrates = subrates;
    }
}
