package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MainApp {
    public static void main(String[] args) throws Exception {
        ClassLoader classLoader = MainApp.class.getClassLoader();
        String csvFilePath = classLoader.getResource("input_data_all.csv").getPath();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Object>> tpchStream = env.addSource(new TpchSourceFunction(csvFilePath));

        // 按类型分流，分别打印
        tpchStream.filter(t -> t.f0.equals("lineitem"))
                .map(t -> (Lineitem)t.f1)
                .print("Lineitem")
                .setParallelism(1);

        tpchStream.filter(t -> t.f0.equals("orders"))
                .map(t -> (Orders)t.f1)
                .print("Orders")
                .setParallelism(1);

        tpchStream.filter(t -> t.f0.equals("customer"))
                .map(t -> (Customer)t.f1)
                .print("Customer")
                .setParallelism(1);

        env.execute("Read All TPC-H Tables");
    }
}