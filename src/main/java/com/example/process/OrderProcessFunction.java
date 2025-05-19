package com.example.process;

import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import com.example.model.Order;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 订单流处理器
 * 将客户流与订单流连接并基于业务规则筛选订单
 */
public class OrderProcessFunction extends KeyedCoProcessFunction<Long, Tuple2<Long, String>, Tuple2<Order, String>, Tuple2<Long, String>> {
    
    // 状态定义
    private MapState<Long, List<Long>> customerToOrdersMapping;
    private ValueState<Set<Long>> filteredCustomersState;
    
    // 日期常量
    private static final LocalDate ORDER_DATE_THRESHOLD = LocalDate.parse("1995-03-13", DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    
    // 操作类型常量
    private static final String ADD_OPERATION = "+";
    private static final String REMOVE_OPERATION = "-";

    /**
     * 初始化状态存储
     */
    @Override
    public void open(Configuration config) throws Exception {
        // 初始化客户到订单的映射状态
        MapStateDescriptor<Long, List<Long>> customerToOrdersDescriptor = new MapStateDescriptor<>(
            "customer-orders-mapping", 
            Long.class, 
            (Class<List<Long>>) (Class<?>) List.class
        );
        customerToOrdersMapping = getRuntimeContext().getMapState(customerToOrdersDescriptor);
        
        // 初始化已筛选客户集合状态
        ValueStateDescriptor<Set<Long>> filteredCustomersDescriptor = new ValueStateDescriptor<>(
            "filtered-customers", 
            (Class<Set<Long>>) (Class<?>) Set.class
        );
        filteredCustomersState = getRuntimeContext().getState(filteredCustomersDescriptor);
    }

    /**
     * 处理客户流数据
     */
    @Override
    public void processElement1(Tuple2<Long, String> customerRecord, Context ctx, Collector<Tuple2<Long, String>> out) throws Exception {
        Long customerId = customerRecord.f0;
        String operation = customerRecord.f1;
        
        // 获取或初始化已筛选客户集合
        Set<Long> filteredCustomers = filteredCustomersState.value();
        if (filteredCustomers == null) {
            filteredCustomers = new HashSet<>();
        }
        
        // 根据操作类型更新客户集合
        updateCustomerSet(filteredCustomers, customerId, operation);
        filteredCustomersState.update(filteredCustomers);
        
        // 如果该客户有关联订单，则输出这些订单
        forwardRelatedOrders(customerId, operation, out);
    }

    /**
     * 处理订单流数据
     */
    @Override
    public void processElement2(Tuple2<Order, String> orderRecord, Context ctx, Collector<Tuple2<Long, String>> out) throws Exception {
        Order order = orderRecord.f0;
        String operation = orderRecord.f1;
        
        // 提取订单信息
        Long customerId = order.getOCustkey();
        Long orderId = order.getOOrderkey();
        LocalDate orderDate = order.getOOrderdate();
        
        // 只处理符合日期条件的订单
        if (orderDate.isBefore(ORDER_DATE_THRESHOLD)) {
            // 更新订单映射
            updateOrderMapping(customerId, orderId, operation);
            
            // 检查该客户是否已被筛选
            checkAndForwardOrder(customerId, orderId, operation, out);
        }
    }
    
    /**
     * 更新客户集合
     */
    private void updateCustomerSet(Set<Long> customers, Long customerId, String operation) {
        if (ADD_OPERATION.equals(operation)) {
            customers.add(customerId);
        } else if (REMOVE_OPERATION.equals(operation)) {
            customers.remove(customerId);
        }
    }
    
    /**
     * 转发与客户关联的所有订单
     */
    private void forwardRelatedOrders(Long customerId, String operation, Collector<Tuple2<Long, String>> out) throws Exception {
        List<Long> orderIds = customerToOrdersMapping.get(customerId);
        if (orderIds != null && !orderIds.isEmpty()) {
            for (Long orderId : orderIds) {
                out.collect(new Tuple2<>(orderId, operation));
            }
        }
    }
    
    /**
     * 更新客户到订单的映射
     */
    private void updateOrderMapping(Long customerId, Long orderId, String operation) throws Exception {
        List<Long> orderIds = customerToOrdersMapping.get(customerId);
        
        if (ADD_OPERATION.equals(operation)) {
            if (orderIds == null) {
                orderIds = new ArrayList<>();
            }
            if (!orderIds.contains(orderId)) {
                orderIds.add(orderId);
                customerToOrdersMapping.put(customerId, orderIds);
            }
        } else if (REMOVE_OPERATION.equals(operation)) {
            if (orderIds != null) {
                orderIds.remove(orderId);
                if (orderIds.isEmpty()) {
                    customerToOrdersMapping.remove(customerId);
                } else {
                    customerToOrdersMapping.put(customerId, orderIds);
                }
            }
        }
    }
    
    /**
     * 检查客户是否已被筛选并转发相应订单
     */
    private void checkAndForwardOrder(Long customerId, Long orderId, String operation, Collector<Tuple2<Long, String>> out) throws Exception {
        Set<Long> filteredCustomers = filteredCustomersState.value();
        if (filteredCustomers != null && filteredCustomers.contains(customerId)) {
            out.collect(new Tuple2<>(orderId, operation));
        }
    }
} 