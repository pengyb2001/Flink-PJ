package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MainApp {
    public static void main(String[] args) throws Exception {
        ClassLoader classLoader = MainApp.class.getClassLoader();
        String csvFilePath = classLoader.getResource("input_data_all.csv").getPath();
//        String csvFilePath = "/Users/pengyibo/Desktop/2025-Spring/IP-Flink/Cquirrel-release/DemoTools/DataGenerator/data-1/input_data_all-1gb.csv";

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

        // 3. 使用HASH分区将Customer流和Orders流连接
        // 通过custkey进行连接，保留活跃customer相关且满足日期的orders
        DataStream<Orders> activeOrdersStream = ordersStream
                .keyBy(o -> o.custkey)
                .connect(activeCustomerStream.keyBy(c -> c.custkey))
                .process(new OrdersProcessFunction());

        // 4. 使用HASH分区将Orders流和Lineitem流连接
        // 通过orderkey进行连接，保留能和活跃orders匹配的lineitem
        DataStream<JoinedTuple> joinedStream = lineitemStream
                .keyBy(li -> li.orderkey)
                .connect(activeOrdersStream.keyBy(o -> o.orderkey))
                .process(new LineitemProcessFunction());

        // 5. group by l_orderkey, o_orderdate, o_shippriority，流式增删聚合
        DataStream<String> q3ResultStream = joinedStream
                .keyBy(t -> new GroupKey(t.orderkey, t.orderdate, t.shippriority))
                .process(new GroupByAggregateFunction());

        // 6. 实时写入增删流结果到 txt（每次 run 都 append，可对比）
        q3ResultStream
                .addSink(new AppendFileSink<>("q3result_output-hash.txt"))
                .setParallelism(1);

        // 7. 程序结束时自动输出"Q3最终快照"（所有group，包括revenue=0）
        // 将并行结果收集到一个实例进行处理
        q3ResultStream
                .keyBy(value -> 1) // 使用常量键将所有数据发送到同一个实例
                .addSink(new Q3SnapshotSink())
                .setParallelism(1); // 强制使用单一并行度

        env.execute("Cquirrel Q3 Hash Join Streaming with Final Snapshot");
        // sleep
//        Thread.sleep(1000 * 60 * 60 * 1); // 1 hour
    }
}