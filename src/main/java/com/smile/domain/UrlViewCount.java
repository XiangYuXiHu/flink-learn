package com.smile.domain;

/**
 * @Description
 * @ClassName UrlViewCount
 * @Author smile
 * @date 2022.11.24 21:40
 */
public class UrlViewCount {

    private String url;

    private Long count;

    private Long start;

    private Long end;

    public UrlViewCount() {

    }

    public UrlViewCount(String url, Long count, Long start, Long end) {
        this.url = url;
        this.count = count;
        this.start = start;
        this.end = end;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Long getStart() {
        return start;
    }

    public void setStart(Long start) {
        this.start = start;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    @Override
    public String toString() {
        return "UrlViewCount{" +
                "url='" + url + '\'' +
                ", count=" + count +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}
