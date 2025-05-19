package com.example.process;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.java.tuple.Tuple2;
import com.example.model.Order;
import com.example.util.SharedStateManager;

import java.time.LocalDate;
import java.util.Set;

/**
 * 订单流与客户流连接处理器
 * 统一状态管理版本：使用共享状态管理器实现
 */
public class OrderProcessFunction extends KeyedCoProcessFunction<Long, Tuple2<Long, String>, Tuple2<Order, String>, Tuple2<Long, String>> {
    
    // 操作类型常量
    private static final String OP_ADD = "+";
    private static final String OP_DEL = "-";
    
    // 日期阈值
    private static final LocalDate DATE_THRESHOLD = LocalDate.parse("1995-03-13");
    
    // 共享状态管理器
    private SharedStateManager stateManager;
    
    @Override
    public void open(Configuration config) throws Exception {
        // 初始化共享状态管理器
        stateManager = new SharedStateManager(getRuntimeContext());
    }

    /**
     * 处理客户事件
     */
    @Override
    public void processElement1(Tuple2<Long, String> customerRecord, Context ctx, Collector<Tuple2<Long, String>> out) throws Exception {
        Long customerId = customerRecord.f0;
        String operation = customerRecord.f1;
        
        // 根据操作类型更新客户状态
        if (operation.equals(OP_ADD)) {
            stateManager.addFilteredCustomer(customerId);
        } else if (operation.equals(OP_DEL)) {
            stateManager.removeFilteredCustomer(customerId);
        }
        
        // 获取并输出客户的所有关联订单
        Set<Long> orderIds = stateManager.getCustomerOrders(customerId);
        for (Long orderId : orderIds) {
            out.collect(new Tuple2<>(orderId, operation));
        }
    }

    /**
     * 处理订单事件
     */
    @Override
    public void processElement2(Tuple2<Order, String> orderRecord, Context ctx, Collector<Tuple2<Long, String>> out) throws Exception {
        Order order = orderRecord.f0;
        String operation = orderRecord.f1;
        
        // 提取订单信息
        Long customerId = order.getOCustkey();
        Long orderId = order.getOOrderkey();
        LocalDate orderDate = order.getOOrderdate();
        
        // 跳过不符合日期条件的订单
        if (!orderDate.isBefore(DATE_THRESHOLD)) {
            return;
        }
        
        // 根据操作类型处理订单
        if (operation.equals(OP_ADD)) {
            // 关联订单和客户
            stateManager.linkOrderToCustomer(orderId, customerId);
        } else if (operation.equals(OP_DEL)) {
            // 删除订单及其关联信息
            stateManager.removeOrder(orderId);
        }
        
        // 检查客户是否已被筛选，如果是则输出订单
        if (stateManager.isCustomerFiltered(customerId)) {
            out.collect(new Tuple2<>(orderId, operation));
        }
    }
} 