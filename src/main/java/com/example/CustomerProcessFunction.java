package com.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 该函数负责维护每个customer的活跃状态，只输出真正活跃的customer（AUTOMOBILE）。
 * 后续只需要与这个流进行join即可。
 */
public class CustomerProcessFunction extends KeyedProcessFunction<String, Customer, Customer> {
    // 活跃状态
    private transient ValueState<Boolean> isActiveState;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("isActive", Boolean.class);
        isActiveState = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(Customer customer, Context ctx, Collector<Customer> out) throws Exception {
        // 支持插入和删除（后续可以支持UPDATE）
        if ("INSERT".equalsIgnoreCase(customer.opType)) {
            // 只处理AUTOMOBILE
            if (customer.mktsegment != null && customer.mktsegment.equalsIgnoreCase("AUTOMOBILE")) {
                Boolean prev = isActiveState.value();
                if (prev == null || !prev) {
                    // 首次活跃，输出到下游
                    isActiveState.update(true);
                    out.collect(customer);
                }
                // 如果已活跃，不重复输出
            }
            // 不是AUTOMOBILE则不输出，状态不变
        } else if ("DELETE".equalsIgnoreCase(customer.opType)) {
            // 如果之前是活跃，现在变为非活跃
            Boolean prev = isActiveState.value();
            if (prev != null && prev) {
                isActiveState.update(false);
                // 后续如需通知下游取消活跃可输出一个特殊标志
                // 可以自定义Customer类型输出或发撤销信号
            }
        }
        // 后续如需支持UPDATE，在这里补充逻辑
    }
}