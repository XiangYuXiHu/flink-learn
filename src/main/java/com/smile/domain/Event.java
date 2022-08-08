package com.smile.domain;

/**
 * @Description
 * @ClassName Event
 * @Author smile
 * @date 2022.08.08 22:18
 */
public class Event {

    private String user;
    private String url;
    private Long timeMillis;

    public Event() {

    }

    public Event(String user, String url, Long timeMillis) {
        this.user = user;
        this.url = url;
        this.timeMillis = timeMillis;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getTimeMillis() {
        return timeMillis;
    }

    public void setTimeMillis(Long timeMillis) {
        this.timeMillis = timeMillis;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timeMillis=" + timeMillis +
                '}';
    }
}
