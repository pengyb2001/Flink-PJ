package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.concurrent.atomic.AtomicInteger;

public class MainApp {
    public static void main(String[] args) throws Exception {
        ClassLoader classLoader = MainApp.class.getClassLoader();
        String csvFilePath = classLoader.getResource("input_data_all.csv").getPath();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Object>> tpchStream = env.addSource(new TpchSourceFunction(csvFilePath));

        // 1. 提取customer流
        DataStream<Customer> customerStream = tpchStream
                .filter(t -> t.f0.equals("customer"))
                .map(t -> (Customer) t.f1);
        // 2. 提取orders流
        DataStream<Orders> ordersStream = tpchStream
                .filter(t -> t.f0.equals("orders"))
                .map(t -> (Orders) t.f1);
        // 3. 提取lineitem流
        DataStream<Lineitem> lineitemStream = tpchStream
                .filter(t -> t.f0.equals("lineitem"))
                .map(t -> (Lineitem) t.f1);

        // 随机采样并打印10条原始customer数据（实际是前10条，流式不可随机访问）
        // 可以用AtomicInteger计数，仅输出前10条
//        AtomicInteger counter = new AtomicInteger(0);
//        customerStream
//                .filter(c -> counter.getAndIncrement() < 10)
//                .print("SampleCustomer").setParallelism(1);

        // 4. 经process处理后只输出活跃customer
        DataStream<Customer> activeCustomerStream = customerStream
                .keyBy(c -> c.custkey)
                .process(new CustomerProcessFunction());

        activeCustomerStream.print("ActiveCustomer").setParallelism(1);

        // 随机取样打印Orders和Lineitem流前5条
        // 这里可以用AtomicInteger计数，仅输出前5条
        AtomicInteger orderCounter = new AtomicInteger(0);
        ordersStream
                .filter(o -> orderCounter.getAndIncrement() < 5)
                .print("SampleOrders").setParallelism(1);
        AtomicInteger lineitemCounter = new AtomicInteger(0);
        lineitemStream
                .filter(l -> lineitemCounter.getAndIncrement() < 5)
                .print("SampleLineitem").setParallelism(1);


        env.execute("Cquirrel Customer Process Demo with Sampling");
    }
}