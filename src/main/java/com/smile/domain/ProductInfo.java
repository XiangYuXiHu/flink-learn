package com.smile.domain;

import java.io.Serializable;

/**
 * @Description
 * @ClassName ProductInfo
 * @Author smile
 * @date 2022.05.25 10:16
 */
public class ProductInfo implements Serializable {

    private String categoryName;

    private Integer categoryType;

    public ProductInfo() {

    }

    public ProductInfo(String categoryName, Integer categoryType) {
        this.categoryName = categoryName;
        this.categoryType = categoryType;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public Integer getCategoryType() {
        return categoryType;
    }

    public void setCategoryType(Integer categoryType) {
        this.categoryType = categoryType;
    }

    @Override
    public String toString() {
        return "ProductInfo{" +
                "categoryName='" + categoryName + '\'' +
                ", categoryType='" + categoryType + '\'' +
                '}';
    }
}
