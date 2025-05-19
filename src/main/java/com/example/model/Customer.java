package com.example.model;

import java.util.Objects;

/**
 * 客户数据实体
 * 实现TPC-H业务模型中的用户信息，包含识别号和市场分类
 */
public class Customer {
    // 实体属性
    private final long clientId;        // 客户唯一标识
    private final String marketSegment; // 市场细分类别

    /**
     * 创建一个客户实体
     * 
     * @param clientId 客户标识号
     * @param marketSegment 所属市场类别
     */
    public Customer(long clientId, String marketSegment) {
        this.clientId = clientId;
        this.marketSegment = Objects.requireNonNull(marketSegment, "市场分类不能为空");
    }

    /**
     * 获取客户标识符
     * 
     * @return 客户ID
     */
    public long getCCustkey() {
        return this.clientId;
    }

    /**
     * 获取市场分类信息
     * 
     * @return 市场分类
     */
    public String getCMktsegment() {
        return this.marketSegment;
    }

    /**
     * 解析原始数据创建客户对象
     * 
     * @param rawData 原始数据字符串
     * @return 客户实体实例
     */
    public static Customer fromString(String rawData) {
        // 数据分割处理
        String[] dataElements = rawData.split("\\|");
        
        if (dataElements.length < 7) {
            throw new IllegalArgumentException("客户数据格式不正确: " + rawData);
        }
        
        // 解析客户ID和市场分类字段
        long id = Long.parseLong(dataElements[0].trim());
        String segment = dataElements[6].trim();
        
        // 创建并返回新实例
        return new Customer(id, segment);
    }
    
    /**
     * 生成对象的字符串表示
     */
    @Override
    public String toString() {
        return "Customer[id=" + clientId + ", segment=" + marketSegment + "]";
    }
    
    /**
     * 比较两个客户对象是否相等
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Customer)) return false;
        
        Customer other = (Customer) obj;
        return this.clientId == other.clientId && 
               Objects.equals(this.marketSegment, other.marketSegment);
    }
    
    /**
     * 生成哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(clientId, marketSegment);
    }
} 