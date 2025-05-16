package com.example;

import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class LineitemProcessFunction extends KeyedBroadcastProcessFunction<String, Lineitem, Orders, JoinedTuple> {

    public static final MapStateDescriptor<String, Orders> ORDERS_BROADCAST_STATE_DESC =
            new MapStateDescriptor<>("ordersBroadcastState", String.class, Orders.class);

    // 记录当前 lineitem 是否“活跃”（方便 delete 流时判断）
    private transient ValueState<Boolean> isLineitemActive;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<Boolean> desc = new ValueStateDescriptor<>("isLineitemActive", Boolean.class);
        isLineitemActive = getRuntimeContext().getState(desc);
    }

    // Lineitem流
    @Override
    public void processElement(Lineitem li, ReadOnlyContext ctx, Collector<JoinedTuple> out) throws Exception {
        ReadOnlyBroadcastState<String, Orders> ordersState = ctx.getBroadcastState(ORDERS_BROADCAST_STATE_DESC);

        // 必须 order 先活跃
        Orders order = ordersState.get(li.orderkey);
        if (order == null) {
            return; // 没有关联 order，不处理
        }

        // 只处理 l_shipdate > 1995-03-13
        if (li.shipdate.compareTo("1995-03-13") <= 0) {
            return;
        }

        // insert
        if ("INSERT".equalsIgnoreCase(li.opType)) {
            Boolean prev = isLineitemActive.value();
            if (prev == null || !prev) {
                isLineitemActive.update(true);
                out.collect(new JoinedTuple(li, order, "INSERT"));
            }
        }
        // delete
        else if ("DELETE".equalsIgnoreCase(li.opType)) {
            Boolean prev = isLineitemActive.value();
            if (prev != null && prev) {
                isLineitemActive.update(false);
                out.collect(new JoinedTuple(li, order, "DELETE"));
            }
        }
    }

    // Orders流，广播：增删活跃 order
    @Override
    public void processBroadcastElement(Orders order, Context ctx, Collector<JoinedTuple> out) throws Exception {
        BroadcastState<String, Orders> ordersState = ctx.getBroadcastState(ORDERS_BROADCAST_STATE_DESC);
        if ("INSERT".equalsIgnoreCase(order.opType)) {
            ordersState.put(order.orderkey, order);
        } else if ("DELETE".equalsIgnoreCase(order.opType)) {
            ordersState.remove(order.orderkey);
        }
    }
}