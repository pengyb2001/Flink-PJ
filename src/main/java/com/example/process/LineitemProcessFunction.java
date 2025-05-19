package com.example.process;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple2;
import com.example.model.LineItem;
import com.example.util.SharedStateManager;
import com.example.util.SharedStateManager.LineItemData;

import java.time.LocalDate;
import java.util.Map;

/**
 * 订单项与订单连接处理器
 * 统一状态管理版本：使用共享状态管理器实现
 */
public class LineitemProcessFunction extends KeyedCoProcessFunction<Long, Tuple2<Long, String>, Tuple2<LineItem, String>, Tuple4<LocalDate, Double, Double, String>> {
    
    // 操作类型常量
    private static final String OP_ADD = "+";
    private static final String OP_DEL = "-";
    
    // 日期阈值
    private static final LocalDate DATE_THRESHOLD = LocalDate.parse("1995-03-13");
    
    // 共享状态管理器
    private SharedStateManager stateManager;
    
    /**
     * 初始化方法
     */
    @Override
    public void open(Configuration params) throws Exception {
        // 初始化共享状态管理器
        stateManager = new SharedStateManager(getRuntimeContext());
    }
    
    /**
     * 处理订单事件
     */
    @Override
    public void processElement1(Tuple2<Long, String> orderEvent, Context ctx, 
                             Collector<Tuple4<LocalDate, Double, Double, String>> out) throws Exception {
        Long orderId = orderEvent.f0;
        String eventType = orderEvent.f1;
        
        // 更新订单有效性状态
        stateManager.markOrderValid(orderId, OP_ADD.equals(eventType));
        
        // 获取所有相关行项目并输出
        Map<String, LineItemData> lineItems = stateManager.getOrderLineItems(orderId);
        for (LineItemData item : lineItems.values()) {
            out.collect(new Tuple4<>(
                item.getShipDate(), 
                item.getExtPrice(), 
                item.getDiscountRate(), 
                eventType
            ));
        }
    }
    
    /**
     * 处理行项目事件
     */
    @Override
    public void processElement2(Tuple2<LineItem, String> lineItemEvent, Context ctx, 
                             Collector<Tuple4<LocalDate, Double, Double, String>> out) throws Exception {
        LineItem item = lineItemEvent.f0;
        String eventType = lineItemEvent.f1;
        
        // 提取行项目数据
        Long orderId = item.getLOrderkey();
        LocalDate shipDate = item.getLShipDate();
        double extPrice = item.getLExtendedprice();
        double discount = item.getLDiscount();
        
        // 检查发货日期条件（需要晚于边界日期）
        if (shipDate.compareTo(DATE_THRESHOLD) <= 0) {
            return; // 不满足条件，直接跳过
        }
        
        // 创建行项目数据对象
        LineItemData itemData = new LineItemData(shipDate, extPrice, discount);
        
        // 生成唯一标识
        String itemKey = generateItemKey(shipDate, extPrice, discount);
        
        // 根据操作类型处理行项目
        if (OP_ADD.equals(eventType)) {
            // 添加行项目到订单
            stateManager.addLineItemToOrder(orderId, itemKey, itemData);
        } else if (OP_DEL.equals(eventType)) {
            // 从订单中移除行项目
            stateManager.removeLineItemFromOrder(orderId, itemKey);
        }
        
        // 检查订单是否有效，如果有效则发送该行项目
        if (stateManager.isOrderValid(orderId)) {
            out.collect(new Tuple4<>(
                shipDate, extPrice, discount, eventType
            ));
        }
    }
    
    /**
     * 生成行项目唯一标识
     */
    private String generateItemKey(LocalDate shipDate, double extPrice, double discount) {
        return "item:" + (31 * (31 * shipDate.hashCode() + Double.hashCode(extPrice)) + Double.hashCode(discount));
    }
} 