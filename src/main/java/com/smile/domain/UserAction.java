package com.smile.domain;

import java.math.BigDecimal;

/**
 * @Description
 * @ClassName UserAction
 * @Author smile
 * @date 2022.08.07 19:54
 */
public class UserAction {

    private String userId;
    private Integer userNo;
    private String action;
    private String product;
    private BigDecimal price;

    public UserAction(String userId, Integer userNo, String action, String product, BigDecimal price) {
        this.userId = userId;
        this.userNo = userNo;
        this.action = action;
        this.product = product;
        this.price = price;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public Integer getUserNo() {
        return userNo;
    }

    public void setUserNo(Integer userNo) {
        this.userNo = userNo;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getProduct() {
        return product;
    }

    public void setProduct(String product) {
        this.product = product;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "UserAction{" +
                "userId='" + userId + '\'' +
                ", userNo=" + userNo +
                ", action='" + action + '\'' +
                ", product='" + product + '\'' +
                ", price=" + price +
                '}';
    }
}
