package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MainApp {
    public static void main(String[] args) throws Exception {
        ClassLoader classLoader = MainApp.class.getClassLoader();
        String csvFilePath = classLoader.getResource("input_data_all.csv").getPath();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. 三表解析流
        DataStream<Tuple2<String, Object>> tpchStream = env.addSource(new TpchSourceFunction(csvFilePath));

        DataStream<Customer> customerStream = tpchStream
                .filter(t -> t.f0.equals("customer"))
                .map(t -> (Customer) t.f1);

        DataStream<Orders> ordersStream = tpchStream
                .filter(t -> t.f0.equals("orders"))
                .map(t -> (Orders) t.f1);

        DataStream<Lineitem> lineitemStream = tpchStream
                .filter(t -> t.f0.equals("lineitem"))
                .map(t -> (Lineitem) t.f1);

        // 2. 只保留活跃Customer（支持insert/delete，带opType）
        DataStream<Customer> activeCustomerStream = customerStream
                .keyBy(c -> c.custkey)
                .process(new CustomerProcessFunction());

        // 3. 将活跃customer流转为broadcast流，供orders join
        BroadcastStream<Customer> customerBroadcast = activeCustomerStream
                .broadcast(OrdersProcessFunction.CUSTOMER_BROADCAST_STATE_DESC);

        // 4. 只保留活跃customer相关且满足日期的orders（支持insert/delete）
        DataStream<Orders> activeOrdersStream = ordersStream
                .keyBy(o -> o.custkey)
                .connect(customerBroadcast)
                .process(new OrdersProcessFunction());

        // 5. 将活跃orders流转为broadcast流，供lineitem join
        BroadcastStream<Orders> ordersBroadcast = activeOrdersStream
                .broadcast(LineitemProcessFunction.ORDERS_BROADCAST_STATE_DESC);

        // 6. join lineitem和活跃orders，仅保留完全有效行
        DataStream<JoinedTuple> joinedStream = lineitemStream
                .keyBy(li -> li.orderkey)
                .connect(ordersBroadcast)
                .process(new LineitemProcessFunction());

        // 7. group by l_orderkey, o_orderdate, o_shippriority，流式增删聚合
        DataStream<String> q3ResultStream = joinedStream
                .keyBy(t -> new GroupKey(t.orderkey, t.orderdate, t.shippriority))
                .process(new GroupByAggregateFunction());

        // 8. 实时写入增删流结果到 txt（每次 run 都 append，可对比）
        q3ResultStream
                .addSink(new AppendFileSink<>("q3result_output.txt"))
                .setParallelism(1);

        // 9. 程序结束时自动输出“Q3最终快照”（所有group，包括revenue=0）
        q3ResultStream
                .addSink(new Q3SnapshotSink())
                .setParallelism(1);

        env.execute("Cquirrel Q3 Full Incremental Streaming with Final Snapshot");
    }
}