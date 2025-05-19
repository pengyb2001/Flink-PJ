package com.example;

import com.example.model.Customer;
import com.example.model.Order;
import com.example.model.LineItem;
import com.example.process.*;
import com.example.util.LocalDateSerializer;
import com.example.util.ShipDateRevenueSink;
import com.example.util.TpchSourceFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple2;

import java.time.LocalDate;

/**
 * Flink流处理主程序
 * 实现了TPC-H查询的流式处理版本，用于计算特定市场部门客户的订单在特定日期范围内的总收入
 */
public class MainApp {
    public static void main(String[] args) throws Exception {
        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(1);

        // 注册LocalDate序列化器
        env.getConfig().registerTypeWithKryoSerializer(LocalDate.class, LocalDateSerializer.class);

        // 创建文件数据源
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new org.apache.flink.core.fs.Path("/Users/pengyibo/Desktop/2025-Spring/IP-Flink/flink-ref/input_data_all_70percent.csv"))
                .build();

        // 创建输入数据流
        DataStream<String> rawInputStream = env.fromSource(fileSource, WatermarkStrategy.noWatermarks(), "文件数据源");

        // 利用TpchSourceFunction工具类解析并提取三种业务对象流
        Tuple2<Tuple2<SingleOutputStreamOperator<Tuple2<Customer, String>>, SingleOutputStreamOperator<Tuple2<Order, String>>>,
              SingleOutputStreamOperator<Tuple2<LineItem, String>>> allStreams = 
                TpchSourceFunction.extractAllStreams(rawInputStream);
        
        // 获取各个业务对象流
        SingleOutputStreamOperator<Tuple2<Customer, String>> customerStream = allStreams.f0.f0;
        SingleOutputStreamOperator<Tuple2<Order, String>> orderStream = allStreams.f0.f1;
        SingleOutputStreamOperator<Tuple2<LineItem, String>> lineItemStream = allStreams.f1;

        // 第一步：处理Customer对象，筛选出市场部门为AUTOMOBILE的客户
        DataStream<Tuple2<Long, String>> filteredCustomerKeys = customerStream
                .keyBy(tuple -> tuple.f0.getCCustkey())
                .process(new CustomerProcessFunction());

        // 第二步：连接筛选后的客户和订单数据，找出符合条件的订单
        DataStream<Tuple2<Long, String>> relevantOrderKeys = filteredCustomerKeys
                .connect(orderStream)
                .keyBy(
                    customerKey -> customerKey.f0,
                    order -> order.f0.getOCustkey()
                )
                .process(new OrderProcessFunction());

        // 第三步：连接订单和订单明细数据，处理符合条件的订单明细
        DataStream<Tuple4<LocalDate, Double, Double, String>> lineItemResults = relevantOrderKeys
                .connect(lineItemStream)
                .keyBy(
                    orderKey -> orderKey.f0,
                    lineItem -> lineItem.f0.getLOrderkey()
                )
                .process(new LineitemProcessFunction());

        // 第四步：按照发货日期聚合收入
        DataStream<Tuple2<LocalDate, Double>> shipDateRevenueResults = lineItemResults
                .keyBy(value -> value.f0) // 按发货日期分组
                .process(new ShipDateRevenueAggregationFunction())
                .keyBy(value -> value.f0) // 再次按发货日期分组
                .reduce((value1, value2) -> {
                    // 合并具有相同发货日期的结果，取最新的总收入
                    return new Tuple2<>(value1.f0, value2.f1);
                });

        // 第五步：使用自定义接收器整合所有分区的结果并输出
        shipDateRevenueResults
                .addSink(new ShipDateRevenueSink())
                .setParallelism(1);

        // 执行作业
        env.execute("TPC-H流处理作业");
        System.out.println("作业执行完成");
    }
} 