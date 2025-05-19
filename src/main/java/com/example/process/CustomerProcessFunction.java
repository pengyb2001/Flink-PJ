package com.example.process;

import com.example.model.Customer;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * 客户数据处理器
 * 该处理器筛选汽车行业的客户记录
 */
public class CustomerProcessFunction extends KeyedProcessFunction<Long, Tuple2<Customer, String>, Tuple2<Long, String>> {
    
    // 目标市场部门常量
    private static final String TARGET_MARKET_SEGMENT = "AUTOMOBILE";
    
    /**
     * 处理流中的每一条客户记录
     * 
     * @param inputRecord 包含客户数据和事件类型的元组
     * @param ctx 处理函数上下文
     * @param resultCollector 结果收集器
     * @throws Exception 如果处理过程中出现错误
     */
    @Override
    public void processElement(Tuple2<Customer, String> inputRecord, Context ctx, Collector<Tuple2<Long, String>> resultCollector) throws Exception {
        // 从输入记录中提取客户信息和事件类型
        Customer customerData = inputRecord.f0;
        String eventType = inputRecord.f1;
        
        // 检查客户是否属于目标市场部门
        if (TARGET_MARKET_SEGMENT.equals(customerData.getCMktsegment())) {
            // 提取客户ID并与事件类型一起输出
            Long customerId = customerData.getCCustkey();
            resultCollector.collect(new Tuple2<>(customerId, eventType));
        }
    }
} 