package com.example;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class GroupByAggregateFunction extends KeyedProcessFunction<GroupKey, JoinedTuple, String> {
    private transient ValueState<Double> sumRevenue;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Double> desc = new ValueStateDescriptor<>("sumRevenue", Double.class, 0.0);
        sumRevenue = getRuntimeContext().getState(desc);
    }

    @Override
    public void processElement(JoinedTuple value, Context ctx, Collector<String> out) throws Exception {
        double curr = sumRevenue.value();
        double delta = value.extendedprice * (1 - value.discount);

        if ("INSERT".equalsIgnoreCase(value.opType)) {
            curr += delta;
        } else if ("DELETE".equalsIgnoreCase(value.opType)) {
            curr -= delta;
        }
        // 小于一定量为0
        if (curr < 0.00000001) {
            curr = 0.0;
        }
        sumRevenue.update(curr);

        // 输出最新聚合值（你可按需只输出INSERT时、变化时、或周期性输出）
        out.collect(
                "Q3Result: l_orderkey=" + value.orderkey +
                        ", o_orderdate=" + value.orderdate +
                        ", o_shippriority=" + value.shippriority +
                        ", revenue=" + curr
        );
    }
}