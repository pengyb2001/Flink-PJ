package com.example;

import com.example.model.Customer;
import com.example.model.Order;
import com.example.model.LineItem;
import com.example.process.*;
import com.example.util.LocalDateSerializer;
import com.example.util.ShipDateRevenueSink;
import com.example.util.TpchSourceFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Flink流处理主程序
 * 实现了TPC-H查询的流式处理版本，用于计算特定市场部门客户的订单在特定日期范围内的总收入
 */
public class MainApp {
    // 基本路径配置
    private static final String BASE_PATH = "/Users/pengyibo/Desktop/2025-Spring/IP-Flink/Cquirrel-release/DemoTools/DataGenerator/data-1/";

    // 测试数据集配置
    private static final List<String> DATA_SETS = Arrays.asList(
        "input_data_all-1gb_1percent.csv",
        "input_data_all-1gb_10percent.csv",
        "input_data_all-1gb_50percent.csv",
        "input_data_all-1gb_60percent.csv",
        "input_data_all-1gb_70percent.csv",
        "input_data_all-1gb_80percent.csv",
        "input_data_all-1gb_90percent.csv",
        "input_data_all-1gb.csv"
    );
    
    // 并行度配置
    private static final List<Integer> PARALLELISM_LEVELS = Arrays.asList(1, 2, 4, 8);
    
    // 测试结果记录
    private static class TestResult {
        String dataSet;
        int parallelism;
        long executionTime;
        
        public TestResult(String dataSet, int parallelism, long executionTime) {
            this.dataSet = dataSet;
            this.parallelism = parallelism;
            this.executionTime = executionTime;
        }
        
        @Override
        public String toString() {
            return dataSet + "," + parallelism + "," + executionTime;
        }
    }
    
    public static void main(String[] args) throws Exception {
        // 判断是否运行性能测试
//        boolean runPerformanceTest = (args.length > 0 && "test".equals(args[0]));
        boolean runPerformanceTest = true; // 默认运行性能测试

        if (runPerformanceTest) {
            // 执行性能测试
            runPerformanceTests();
        } else {
            // 执行单次作业
            String dataFile = "input_data_all-1gb_1percent.csv";
            int parallelism = 8;

            // 如果提供了参数，则使用参数
            if (args.length >= 1) {
                dataFile = args[0];
            }
            if (args.length >= 2) {
                parallelism = Integer.parseInt(args[1]);
            }

            runJob(dataFile, parallelism);
        }
    }

    /**
     * 执行性能测试
     */
    private static void runPerformanceTests() throws Exception {
        List<TestResult> results = new ArrayList<>();
        
        System.out.println("开始性能测试...");
        System.out.println("====================");
        
        // 遍历所有数据集和并行度组合
        for (String dataSet : DATA_SETS) {
            for (int parallelism : PARALLELISM_LEVELS) {
                System.out.printf("测试数据集: %s, 并行度: %d\n", dataSet, parallelism);
                
                // 执行作业并记录时间
                JobExecutionResult result = runJob(dataSet, parallelism);
                long executionTime = result.getNetRuntime();  // Flink 精确执行时间
                
                // 记录结果
                results.add(new TestResult(dataSet, parallelism, executionTime));
                
                System.out.printf("执行时间: %d ms\n", executionTime);
                System.out.println("--------------------");
                
                // 等待一段时间，避免资源竞争
                Thread.sleep(2000);
            }
        }
        
        // 输出结果到CSV文件
        saveResultsToCsv(results);
    }
    
    /**
     * 将测试结果保存到CSV文件
     */
    private static void saveResultsToCsv(List<TestResult> results) throws IOException {
        // 生成带时间戳的文件名
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        String fileName = "performance_test_" + timestamp + ".csv";
        
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
            // 写入CSV头
            writer.write("数据集,并行度,执行时间(ms)");
            writer.newLine();
            
            // 写入测试结果
            for (TestResult result : results) {
                writer.write(result.toString());
                writer.newLine();
            }
        }
        
        System.out.println("测试结果已保存到: " + fileName);
    }
    
    /**
     * 执行单次作业
     */
    private static JobExecutionResult runJob(String dataFile, int parallelism) throws Exception {
        // 设置流执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setParallelism(parallelism);

        // 注册LocalDate序列化器
        env.getConfig().registerTypeWithKryoSerializer(LocalDate.class, LocalDateSerializer.class);

        // 创建文件数据源
        String filePath = BASE_PATH + dataFile;
        FileSource<String> fileSource = FileSource
                .forRecordStreamFormat(new TextLineInputFormat(), new Path(filePath))
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
        // 为测试文件添加前缀，避免覆盖
        String outputPrefix = "test_" + dataFile.replace(".csv", "") + "_p" + parallelism + "_";
        shipDateRevenueResults
                .addSink(new ShipDateRevenueSink(outputPrefix))
                .setParallelism(1);

        // 执行作业
        JobExecutionResult result = env.execute("TPC-H流处理作业 [" + dataFile + ", 并行度=" + parallelism + "]");
        System.out.println("作业执行完成: " + dataFile + ", 并行度=" + parallelism);
        
        return result;
    }
} 