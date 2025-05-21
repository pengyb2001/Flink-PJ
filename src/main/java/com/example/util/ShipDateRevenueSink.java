package com.example.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * 发货日期收入接收器
 * 用于整合所有分区的结果，并在作业结束时输出最终的发货日期收入统计
 */
public class ShipDateRevenueSink extends RichSinkFunction<Tuple2<LocalDate, Double>> {

    // 存储发货日期到收入的映射
    private Map<LocalDate, Double> shipDateRevenueMap;
    
    // 输出文件前缀
    private final String filePrefix;
    
    /**
     * 默认构造函数
     */
    public ShipDateRevenueSink() {
        this("result_");
    }
    
    /**
     * 带前缀的构造函数
     * 
     * @param filePrefix 输出文件前缀
     */
    public ShipDateRevenueSink(String filePrefix) {
        this.filePrefix = filePrefix;
    }

    /**
     * 初始化接收器
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        shipDateRevenueMap = new HashMap<>();
    }

    /**
     * 处理每条输入记录
     * 
     * @param value 包含发货日期和收入的元组
     * @param context 接收器上下文
     */
    @Override
    public void invoke(Tuple2<LocalDate, Double> value, Context context) throws Exception {
        LocalDate shipDate = value.f0;  // 发货日期
        Double revenue = value.f1;      // 收入金额

        // 更新或添加发货日期收入
        shipDateRevenueMap.put(shipDate, revenue);
    }

    /**
     * 作业结束时输出最终结果
     */
    @Override
    public void close() throws Exception {
        // 打印最终结果
        System.out.println("\n最终发货日期收入统计结果:");
        // 统计有多少条
        System.out.println("总共有 " + shipDateRevenueMap.size() + " 条发货日期收入记录");
        
        // 按照shipdate由小到大排序
        shipDateRevenueMap.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(entry -> {
                    LocalDate date = entry.getKey();
                    Double revenue = entry.getValue();
                    System.out.printf("ShipDate: %s, Revenue: %.2f %n", date, revenue);
                });
                
        // 去掉revenue为0的部分,绝对值小于某个极小常量都视为0
        shipDateRevenueMap.entrySet().removeIf(entry -> Math.abs(entry.getValue()) < 1e-2);
        System.out.println("去除revenue为0的部分后，剩余 " + shipDateRevenueMap.size() + " 条发货日期收入记录");
        System.out.println("----------------------------------------");
        
        // 将结果保存到CSV文件
        saveResultsToCsv();

        // 清理资源
        shipDateRevenueMap.clear();
    }
    
    /**
     * 将结果保存到CSV文件
     */
    private void saveResultsToCsv() {
        try {
            // 生成带时间戳的文件名
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            String fileName = filePrefix + timestamp + ".csv";
            
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName))) {
                // 写入CSV头
                writer.write("发货日期,收入金额");
                writer.newLine();
                
                // 按日期排序写入结果
                shipDateRevenueMap.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> {
                        try {
                            writer.write(entry.getKey() + "," + String.format("%.2f", entry.getValue()));
                            writer.newLine();
                        } catch (IOException e) {
                            System.err.println("写入结果时出错: " + e.getMessage());
                        }
                    });
            }
            
            System.out.println("结果已保存到文件: " + fileName);
        } catch (IOException e) {
            System.err.println("保存结果到CSV文件时出错: " + e.getMessage());
        }
    }
}