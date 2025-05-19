package com.example.process;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;

import java.time.LocalDate;
import java.util.Objects;

/**
 * 日期收入统计处理器
 * 按照发货日期聚合订单项目的收入
 * 计算方式: 累计(扩展价格 × (1-折扣率))
 */
public class ShipDateRevenueAggregationFunction extends ProcessFunction<Tuple4<LocalDate, Double, Double, String>, Tuple2<LocalDate, Double>> {

    // 事件类型常量
    private static final String EVENT_ADDITION = "+";
    private static final String EVENT_REMOVAL = "-";
    
    // 收入记录存储
    private MapState<LocalDate, Double> dateRevenueStore;

    /**
     * 组件初始化
     */
    @Override
    public void open(Configuration config) throws Exception {
        // 创建状态描述符
        MapStateDescriptor<LocalDate, Double> revenueStoreDescriptor = 
            new MapStateDescriptor<>("date-revenue-store", LocalDate.class, Double.class);
            
        // 初始化状态存储
        dateRevenueStore = getRuntimeContext().getMapState(revenueStoreDescriptor);
    }

    /**
     * 处理输入记录
     */
    @Override
    public void processElement(Tuple4<LocalDate, Double, Double, String> inputRecord, 
                              Context ctx, 
                              Collector<Tuple2<LocalDate, Double>> resultCollector) throws Exception {
        // 解析输入数据
        DateRevenueRecord record = extractDataFromInput(inputRecord);
        
        // 计算当前记录的收入贡献
        double revenueContribution = calculateRevenueContribution(record);
        
        // 更新收入统计并获取最新值
        double updatedRevenue = updateRevenueStatistics(record.date, revenueContribution, record.eventType);
        
        // 输出更新后的结果
        emitResult(record.date, updatedRevenue, resultCollector);
    }
    
    /**
     * 从输入元组中提取数据
     */
    private DateRevenueRecord extractDataFromInput(Tuple4<LocalDate, Double, Double, String> input) {
        return new DateRevenueRecord(
            input.f0,  // 发货日期
            input.f1,  // 扩展价格
            input.f2,  // 折扣率
            input.f3   // 事件类型
        );
    }
    
    /**
     * 计算收入贡献值
     */
    private double calculateRevenueContribution(DateRevenueRecord record) {
        // 实现折扣后的实际收入计算
        return record.price * (1.0 - record.discountRate);
    }
    
    /**
     * 更新收入统计并返回最新值
     */
    private double updateRevenueStatistics(LocalDate date, double revenueChange, String eventType) throws Exception {
        // 获取当前累计值，不存在则初始化为0
        double accumulatedRevenue = Optional(dateRevenueStore.get(date)).orElse(0.0);
        
        // 根据事件类型调整收入
        double newRevenue = adjustRevenueByEventType(accumulatedRevenue, revenueChange, eventType);
        
        // 保存更新后的值
        dateRevenueStore.put(date, newRevenue);
        
        return newRevenue;
    }
    
    /**
     * 根据事件类型调整收入值
     */
    private double adjustRevenueByEventType(double currentValue, double changeAmount, String eventType) {
        if (EVENT_ADDITION.equals(eventType)) {
            return currentValue + changeAmount;
        } else if (EVENT_REMOVAL.equals(eventType)) {
            return currentValue - changeAmount;
        }
        return currentValue; // 默认不变
    }
    
    /**
     * 输出结果
     */
    private void emitResult(LocalDate date, double revenue, Collector<Tuple2<LocalDate, Double>> collector) {
        collector.collect(new Tuple2<>(date, revenue));
    }
    
    /**
     * 处理可能为null的值
     */
    private <T> java.util.Optional<T> Optional(T value) {
        return java.util.Optional.ofNullable(value);
    }
    
    /**
     * 内部数据传输记录类
     */
    private static class DateRevenueRecord {
        final LocalDate date;
        final double price;
        final double discountRate;
        final String eventType;
        
        DateRevenueRecord(LocalDate date, double price, double discountRate, String eventType) {
            this.date = date;
            this.price = price;
            this.discountRate = discountRate;
            this.eventType = eventType;
        }
    }
}