package com.example.model;

import java.time.LocalDate;
import java.util.Objects;

/**
 * 订单项目模型
 * 代表交易系统中的单个商品行项记录
 */
public class LineItem {
    // 主要属性
    private final long orderReference;      // 关联订单标识
    private final LocalDate deliveryDate;   // 物流发货日期
    private final double basePrice;         // 项目基础价格
    private final double discountFactor;    // 价格折扣系数
    private final String deliveryMethod;    // 配送方式
    
    /**
     * 创建订单项实例
     * 
     * @param orderReference 关联订单ID
     * @param deliveryDate 预计发货日期
     * @param basePrice 项目基本价格
     * @param discountFactor 折扣比例
     */
    public LineItem(long orderReference, LocalDate deliveryDate, double basePrice, double discountFactor) {
        this.orderReference = orderReference;
        this.deliveryDate = Objects.requireNonNull(deliveryDate, "发货日期不能为空");
        this.basePrice = basePrice;
        this.discountFactor = discountFactor;
        this.deliveryMethod = null; // 此字段在当前业务中未使用
    }
    
    /**
     * 获取关联订单ID
     * 
     * @return 订单引用
     */
    public long getLOrderkey() {
        return orderReference;
    }
    
    /**
     * 获取物流方式
     * 
     * @return 物流方式描述
     */
    public String getLShipmode() {
        return deliveryMethod;
    }
    
    /**
     * 获取项目基础价格
     * 
     * @return 商品价格
     */
    public double getLExtendedprice() {
        return basePrice;
    }
    
    /**
     * 获取价格折扣系数
     * 
     * @return 折扣率
     */
    public double getLDiscount() {
        return discountFactor;
    }
    
    /**
     * 获取发货日期
     * 
     * @return 发货日期
     */
    public LocalDate getLShipDate() {
        return deliveryDate;
    }
    
    /**
     * 计算项目的实际价值
     * 
     * @return 折扣后价格
     */
    public double getActualValue() {
        return basePrice * (1 - discountFactor);
    }
    
    /**
     * 通过解析数据字符串创建项目记录
     * 
     * @param rawText 包含项目数据的原始文本
     * @return 订单项实例
     * @throws IllegalArgumentException 如果数据格式不正确
     */
    public static LineItem fromString(String rawText) {
        // 解析数据字段
        String[] elements = rawText.split("\\|");
        
        if (elements.length < 11) {
            throw new IllegalArgumentException("订单项数据格式不正确: " + rawText);
        }
        
        try {
            // 提取必要字段并解析
            long orderId = Long.parseLong(elements[0].trim());
            LocalDate shipDate = LocalDate.parse(elements[10].trim());
            double price = Double.parseDouble(elements[5].trim());
            double discount = Double.parseDouble(elements[6].trim());
            
            return new LineItem(orderId, shipDate, price, discount);
        } catch (Exception e) {
            throw new IllegalArgumentException("订单项数据解析错误: " + e.getMessage(), e);
        }
    }
    
    /**
     * 生成字符串表示
     */
    @Override
    public String toString() {
        return "LineItem[order=" + orderReference + 
               ", ship=" + deliveryDate + 
               ", price=" + basePrice + 
               ", discount=" + discountFactor + "]";
    }
    
    /**
     * 比较两个订单项对象是否相等
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof LineItem)) return false;
        
        LineItem other = (LineItem) obj;
        return this.orderReference == other.orderReference && 
               Objects.equals(this.deliveryDate, other.deliveryDate) &&
               Double.compare(this.basePrice, other.basePrice) == 0 &&
               Double.compare(this.discountFactor, other.discountFactor) == 0;
    }
    
    /**
     * 生成哈希码
     */
    @Override
    public int hashCode() {
        return Objects.hash(orderReference, deliveryDate, basePrice, discountFactor);
    }
} 