package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.state.MapStateDescriptor;
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

        // 3. 将活跃customer流转为broadcast流
        BroadcastStream<Customer> customerBroadcast = activeCustomerStream
                .broadcast(OrdersProcessFunction.CUSTOMER_BROADCAST_STATE_DESC);

        // 4. 只保留活跃customer相关且满足日期的orders（支持insert/delete）
        DataStream<Orders> activeOrdersStream = ordersStream
                .keyBy(o -> o.custkey)
                .connect(customerBroadcast)
                .process(new OrdersProcessFunction());

        // 5. 验证输出
        activeOrdersStream.print("ActiveOrders").setParallelism(1);

        env.execute("Cquirrel Orders Process Demo");
    }
}