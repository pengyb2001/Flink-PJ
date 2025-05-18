package com.example;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

public class Q3SnapshotSink extends RichSinkFunction<String> implements CheckpointedFunction {
    // key: 分组字段串, value: 最新revenue
    private final Map<String, Double> groupRevenue = new HashMap<>();
    private transient ListState<Tuple2<String, Double>> checkpointedState;

    @Override
    public void invoke(String value, Context context) {
        // value: "Q3Result: l_orderkey=..., o_orderdate=..., o_shippriority=..., revenue=..."
        try {
            if (!value.startsWith("Q3Result:")) return;
            String[] parts = value.split(",");
            String key = parts[0] + "," + parts[1] + "," + parts[2]; // group key
            double revenue = Double.parseDouble(parts[3].split("=")[1].trim());
            groupRevenue.put(key, revenue);
        } catch (Exception e) {
            // ignore
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // 清除之前的状态
        checkpointedState.clear();
        
        // 保存当前状态到检查点
        for (Map.Entry<String, Double> entry : groupRevenue.entrySet()) {
            checkpointedState.add(new Tuple2<>(entry.getKey(), entry.getValue()));
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // 初始化检查点状态
        ListStateDescriptor<Tuple2<String, Double>> descriptor =
                new ListStateDescriptor<>(
                        "q3-revenue-state",
                        TypeInformation.of(new TypeHint<Tuple2<String, Double>>() {}));
                        
        checkpointedState = context.getOperatorStateStore().getListState(descriptor);
        
        // 如果是从故障恢复，则恢复状态
        if (context.isRestored()) {
            for (Tuple2<String, Double> entry : checkpointedState.get()) {
                // 取最新值或累加，取决于业务需求
                groupRevenue.put(entry.f0, entry.f1);
            }
        }
    }

    @Override
    public void close() throws Exception {
        // 获取所有并行实例的最终结果
        // 注意：在实际运行中，这里只会输出单个并行实例的结果
        // 完整结果需要在作业管理器收集所有实例结果
        
        System.out.println("=== Q3 最终快照结果（当前实例）===");
        groupRevenue.entrySet().stream()
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue())) // 按revenue降序
                .forEach(e -> System.out.println(e.getKey() + ", revenue=" + e.getValue()));
        System.out.println("=====================");

        
        super.close();
    }
}