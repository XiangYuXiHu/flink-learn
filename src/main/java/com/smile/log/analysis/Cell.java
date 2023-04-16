package com.smile.log.analysis;

import java.io.Serializable;

/**
 * @Description
 * @ClassName Cell
 * @Author smile
 * @date 2023.04.15 22:53
 */
public class Cell implements Serializable {

    private String level;
    private Long time;
    private String domain;
    private String traffic;

    public String getLevel() {
        return level;
    }

    public void setLevel(String level) {
        this.level = level;
    }

    public Long getTime() {
        return time;
    }

    public void setTime(Long time) {
        this.time = time;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getTraffic() {
        return traffic;
    }

    public void setTraffic(String traffic) {
        this.traffic = traffic;
    }

    @Override
    public String toString() {
        return "Cell{" +
                "level='" + level + '\'' +
                ", time=" + time +
                ", domain='" + domain + '\'' +
                ", traffic='" + traffic + '\'' +
                '}';
    }
}
