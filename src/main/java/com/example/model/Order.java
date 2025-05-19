package com.example.model;

import java.time.LocalDate;
import java.util.Objects;

/**
 * 订单信息模型
 * 表示业务系统中的交易订单数据
 */
public class Order {
    // 订单基本属性
    private final long transactionId;   // 交易唯一标识符
    private final long buyerId;         // 购买方标识符
    private final LocalDate creationDate; // 订单创建日期

    /**
     * 实例化订单对象
     * 
     * @param transactionId 交易ID
     * @param buyerId 购买方ID
     * @param creationDate 订单创建时间
     */
    public Order(long transactionId, long buyerId, LocalDate creationDate) {
        this.transactionId = transactionId;
        this.buyerId = buyerId;
        this.creationDate = Objects.requireNonNull(creationDate, "订单日期不能为空");
    }

    /**
     * 提取订单标识符
     * 
     * @return 订单ID
     */
    public long getOOrderkey() {
        return transactionId;
    }

    /**
     * 提取购买方标识符
     * 
     * @return 购买方ID
     */
    public long getOCustkey() {
        return buyerId;
    }

    /**
     * 提取订单创建日期
     * 
     * @return 创建日期
     */
    public LocalDate getOOrderdate() {
        return creationDate;
    }

    /**
     * 通过解析原始数据创建订单实例
     * 
     * @param sourceData 原始数据文本
     * @return 订单对象实例
     * @throws IllegalArgumentException 如果数据格式不正确
     */
    public static Order fromString(String sourceData) {
        // 解析数据字段
        String[] elements = sourceData.split("\\|");
        
        if (elements.length < 5) {
            throw new IllegalArgumentException("订单数据格式不正确: " + sourceData);
        }
        
        try {
            // 提取所需字段
            long orderId = Long.parseLong(elements[0].trim());
            long customerId = Long.parseLong(elements[1].trim());
            LocalDate orderDate = LocalDate.parse(elements[4].trim());
            
            return new Order(orderId, customerId, orderDate);
        } catch (Exception e) {
            throw new IllegalArgumentException("订单数据解析错误: " + e.getMessage(), e);
        }
    }
    
    /**
     * 生成订单信息的字符串表示
     */
    @Override
    public String toString() {
        return "Order[id=" + transactionId + 
               ", customer=" + buyerId + 
               ", date=" + creationDate + "]";
    }
    
    /**
     * 比较两个订单对象是否相等
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof Order)) return false;
        
        Order other = (Order) obj;
        return this.transactionId == other.transactionId && 
               this.buyerId == other.buyerId && 
               Objects.equals(this.creationDate, other.creationDate);
    }
    
    /**
     * 生成订单对象哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(transactionId, buyerId, creationDate);
    }
} 