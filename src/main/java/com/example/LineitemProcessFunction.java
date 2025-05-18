package com.example;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

// 注意这里泛型：key为String(orderkey)，第一个流为Lineitem，第二个流为Orders
public class LineitemProcessFunction extends KeyedCoProcessFunction<String, Lineitem, Orders, JoinedTuple> {

    // 存储活跃的Orders
    private transient MapState<String, Orders> ordersState;
    
    // 记录Lineitem是否活跃
    private transient MapState<String, Boolean> lineitemActiveState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Orders> ordersStateDesc = 
            new MapStateDescriptor<>("ordersState", String.class, Orders.class);
        ordersState = getRuntimeContext().getMapState(ordersStateDesc);
        
        MapStateDescriptor<String, Boolean> lineitemActiveStateDesc = 
            new MapStateDescriptor<>("lineitemActiveState", String.class, Boolean.class);
        lineitemActiveState = getRuntimeContext().getMapState(lineitemActiveStateDesc);
    }

    // 处理Lineitem流
    @Override
    public void processElement1(Lineitem li, Context ctx, Collector<JoinedTuple> out) throws Exception {
        // 只处理 l_shipdate > 1995-03-13
        if (li.shipdate.compareTo("1995-03-13") <= 0) {
            return;
        }
        
        // 检查是否有匹配的活跃Orders
        Orders order = ordersState.get(li.orderkey);
        if (order == null) {
            return; // 没有关联的活跃订单，不处理
        }
        
        // 生成唯一键用于跟踪此Lineitem状态
        String lineitemKey = li.orderkey + "_" + li.linenumber;
        
        // 处理Lineitem操作
        if ("INSERT".equalsIgnoreCase(li.opType)) {
            Boolean isActive = lineitemActiveState.get(lineitemKey);
            if (isActive == null || !isActive) {
                lineitemActiveState.put(lineitemKey, true);
                out.collect(new JoinedTuple(li, order, "INSERT")); // 输出Join结果
            }
        } else if ("DELETE".equalsIgnoreCase(li.opType)) {
            Boolean isActive = lineitemActiveState.get(lineitemKey);
            if (isActive != null && isActive) {
                lineitemActiveState.put(lineitemKey, false);
                out.collect(new JoinedTuple(li, order, "DELETE")); // 输出删除信号
            }
        }
    }

    // 处理Orders流
    @Override
    public void processElement2(Orders order, Context ctx, Collector<JoinedTuple> out) throws Exception {
        String orderKey = order.orderkey;
        
        if ("INSERT".equalsIgnoreCase(order.opType)) {
            // 对于插入操作，存储订单
            ordersState.put(orderKey, order);
        } else if ("DELETE".equalsIgnoreCase(order.opType)) {
            // 对于删除操作，移除订单
            ordersState.remove(orderKey);
        }
    }
} 