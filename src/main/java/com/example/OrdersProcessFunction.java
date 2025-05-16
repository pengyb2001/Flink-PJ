package com.example;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

// 注意这里泛型：key为custkey，左流为Orders，右流为Customer
public class OrdersProcessFunction extends KeyedBroadcastProcessFunction<String, Orders, Customer, Orders> {

    // 维护活跃customer的BroadcastState
    public static final MapStateDescriptor<String, Customer> CUSTOMER_BROADCAST_STATE_DESC =
            new MapStateDescriptor<>("customerBroadcastState", String.class, Customer.class);

    private transient ValueState<Boolean> isOrderActive;

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Boolean> orderActiveDesc = new ValueStateDescriptor<>("isOrderActive", Boolean.class);
        isOrderActive = getRuntimeContext().getState(orderActiveDesc);
    }

    // 处理Orders流（主流，keyBy了custkey）
    @Override
    public void processElement(Orders order, ReadOnlyContext ctx, Collector<Orders> out) throws Exception {
        ReadOnlyBroadcastState<String, Customer> customerState = ctx.getBroadcastState(CUSTOMER_BROADCAST_STATE_DESC);

        // 1. 仅处理o_orderdate < 1995-03-13
        if (order.orderdate.compareTo("1995-03-13") >= 0) {
            return; // 不满足日期要求，直接忽略
        }
        // 2. 仅处理活跃customer的orders
        Customer activeCustomer = customerState.get(order.custkey);
        if (activeCustomer == null) {
            return; // 没有活跃customer，不处理
        }
        // 3. 仅插入/删除活跃orders
        if ("INSERT".equalsIgnoreCase(order.opType)) {
            Boolean prev = isOrderActive.value();
            if (prev == null || !prev) {
                isOrderActive.update(true);
                out.collect(order); // 新增活跃order输出
            }
        } else if ("DELETE".equalsIgnoreCase(order.opType)) {
            Boolean prev = isOrderActive.value();
            if (prev != null && prev) {
                isOrderActive.update(false);
                // 如需撤销信号，可输出一条带DELETE标志的order
                // order.opType = "DELETE";
                // out.collect(order);
            }
        }
    }

    // 处理Customer流（广播流，实时更新活跃customer索引）
    @Override
    public void processBroadcastElement(Customer customer, Context ctx, Collector<Orders> out) throws Exception {
        BroadcastState<String, Customer> customerState = ctx.getBroadcastState(CUSTOMER_BROADCAST_STATE_DESC);
        if ("INSERT".equalsIgnoreCase(customer.opType)) {
            customerState.put(customer.custkey, customer);
        } else if ("DELETE".equalsIgnoreCase(customer.opType)) {
            customerState.remove(customer.custkey);
        }
    }
}