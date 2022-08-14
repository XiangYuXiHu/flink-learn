package com.smile.domain;

import java.io.Serializable;

/**
 * @Description
 * @ClassName User
 * @Author smile
 * @date 2022.05.06 15:20
 */
public class User implements Serializable {

    public User() {

    }

    public User(String userId, String username) {
        this.userId = userId;
        this.username = username;
    }

    private String userId;

    private String username;

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    @Override
    public String toString() {
        return "User{" +
                "userId='" + userId + '\'' +
                ", username='" + username + '\'' +
                '}';
    }
}
