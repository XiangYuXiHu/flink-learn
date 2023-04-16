package com.smile.log.analysis;

import java.io.Serializable;

/**
 * @Description
 * @ClassName Elem
 * @Author smile
 * @date 2023.04.15 22:54
 */
public class Elem implements Serializable {

    private String time;
    private String domain;
    private int traffic;

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public int getTraffic() {
        return traffic;
    }

    public void setTraffic(int traffic) {
        this.traffic = traffic;
    }

    @Override
    public String toString() {
        return "Elem{" +
                "time='" + time + '\'' +
                ", domain='" + domain + '\'' +
                ", traffic=" + traffic +
                '}';
    }
}
