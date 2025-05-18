package com.example;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

// 注意这里泛型：key为String(custkey)，第一个流为Orders，第二个流为Customer
public class OrdersProcessFunction extends KeyedCoProcessFunction<String, Orders, Customer, Orders> {

    // 存储活跃的Customer
    private transient MapState<String, Customer> customerState;
    
    // 存储每个Order是否活跃
    private transient MapState<String, Boolean> orderActiveState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Customer> customerStateDesc = 
            new MapStateDescriptor<>("customerState", String.class, Customer.class);
        customerState = getRuntimeContext().getMapState(customerStateDesc);
        
        MapStateDescriptor<String, Boolean> orderActiveStateDesc = 
            new MapStateDescriptor<>("orderActiveState", String.class, Boolean.class);
        orderActiveState = getRuntimeContext().getMapState(orderActiveStateDesc);
    }

    // 处理Orders流
    @Override
    public void processElement1(Orders order, Context ctx, Collector<Orders> out) throws Exception {
        // 1. 仅处理o_orderdate < 1995-03-13
        if (order.orderdate.compareTo("1995-03-13") >= 0) {
            return; // 不满足日期要求，直接忽略
        }
        
        // 2. 检查是否有匹配的活跃Customer
        boolean hasActiveCustomer = false;
        for (Customer customer : customerState.values()) {
            if (customer.custkey.equals(order.custkey) && 
                "AUTOMOBILE".equals(customer.mktsegment)) {
                hasActiveCustomer = true;
                break;
            }
        }
        
        if (!hasActiveCustomer) {
            return; // 没有活跃customer，不处理
        }

        // 3. 处理订单操作
        String orderKey = order.orderkey;
        if ("INSERT".equalsIgnoreCase(order.opType)) {
            Boolean isActive = orderActiveState.get(orderKey);
            if (isActive == null || !isActive) {
                orderActiveState.put(orderKey, true);
                out.collect(order); // 输出活跃订单
            }
        } else if ("DELETE".equalsIgnoreCase(order.opType)) {
            Boolean isActive = orderActiveState.get(orderKey);
            if (isActive != null && isActive) {
                orderActiveState.put(orderKey, false);
                out.collect(order); // 输出删除订单的信号
            }
        }
    }

    // 处理Customer流
    @Override
    public void processElement2(Customer customer, Context ctx, Collector<Orders> out) throws Exception {
        String custKey = customer.custkey;
        
        if ("INSERT".equalsIgnoreCase(customer.opType)) {
            // 只存储符合条件的Customer
            if ("AUTOMOBILE".equals(customer.mktsegment)) {
                customerState.put(custKey, customer);
            }
        } else if ("DELETE".equalsIgnoreCase(customer.opType)) {
            customerState.remove(custKey);
        }
    }
} 