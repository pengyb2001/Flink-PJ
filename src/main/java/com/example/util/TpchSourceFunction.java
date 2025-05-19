package com.example.util;

import com.example.model.Customer;
import com.example.model.Order;
import com.example.model.LineItem;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * TPC-H数据源功能类
 * 提供从原始数据流中提取并转换为业务对象的工具方法
 */
public class TpchSourceFunction {

    /**
     * 从原始数据流中提取Customer对象流
     * 
     * @param dataRecordStream 原始数据记录流
     * @return Customer对象流
     */
    public static SingleOutputStreamOperator<Tuple2<Customer, String>> extractCustomerStream(
            DataStream<ParseOriginalData> dataRecordStream) {
        return dataRecordStream
                .filter(record -> record.getTableType().equals("CU"))
                .map(new MapFunction<ParseOriginalData, Tuple2<Customer, String>>() {
                    @Override
                    public Tuple2<Customer, String> map(ParseOriginalData record) {
                        return new Tuple2<>(Customer.fromString(record.getData()), record.getType());
                    }
                });
    }

    /**
     * 从原始数据流中提取Order对象流
     * 
     * @param dataRecordStream 原始数据记录流
     * @return Order对象流
     */
    public static SingleOutputStreamOperator<Tuple2<Order, String>> extractOrderStream(
            DataStream<ParseOriginalData> dataRecordStream) {
        return dataRecordStream
                .filter(record -> record.getTableType().equals("OR"))
                .map(new MapFunction<ParseOriginalData, Tuple2<Order, String>>() {
                    @Override
                    public Tuple2<Order, String> map(ParseOriginalData record) {
                        return new Tuple2<>(Order.fromString(record.getData()), record.getType());
                    }
                });
    }

    /**
     * 从原始数据流中提取LineItem对象流
     * 
     * @param dataRecordStream 原始数据记录流
     * @return LineItem对象流
     */
    public static SingleOutputStreamOperator<Tuple2<LineItem, String>> extractLineItemStream(
            DataStream<ParseOriginalData> dataRecordStream) {
        return dataRecordStream
                .filter(record -> record.getTableType().equals("LI"))
                .map(new MapFunction<ParseOriginalData, Tuple2<LineItem, String>>() {
                    @Override
                    public Tuple2<LineItem, String> map(ParseOriginalData record) {
                        return new Tuple2<>(LineItem.fromString(record.getData()), record.getType());
                    }
                });
    }

    /**
     * 从原始输入字符串流直接提取三种业务对象流
     * 
     * @param rawInputStream 原始输入字符串流
     * @return 包含三种业务对象流的元组
     */
    public static Tuple2<Tuple2<SingleOutputStreamOperator<Tuple2<Customer, String>>, 
                               SingleOutputStreamOperator<Tuple2<Order, String>>>,
                         SingleOutputStreamOperator<Tuple2<LineItem, String>>> extractAllStreams(
            DataStream<String> rawInputStream) {
        
        // 解析原始数据记录
        DataStream<ParseOriginalData> dataRecordStream = rawInputStream
                .map(line -> ParseOriginalData.fromString(line));
        
        // 提取三种业务对象流
        SingleOutputStreamOperator<Tuple2<Customer, String>> customerStream = 
                extractCustomerStream(dataRecordStream);
        
        SingleOutputStreamOperator<Tuple2<Order, String>> orderStream = 
                extractOrderStream(dataRecordStream);
        
        SingleOutputStreamOperator<Tuple2<LineItem, String>> lineItemStream = 
                extractLineItemStream(dataRecordStream);
        
        return new Tuple2<>(
                new Tuple2<>(customerStream, orderStream),
                lineItemStream
        );
    }
} 