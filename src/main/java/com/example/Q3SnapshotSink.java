package com.example;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.util.HashMap;
import java.util.Map;

public class Q3SnapshotSink extends RichSinkFunction<String> {
    // key: 分组字段串, value: 最新revenue
    private final Map<String, Double> groupRevenue = new HashMap<>();

    @Override
    public synchronized void invoke(String value, Context context) {
        // value: "Q3Result: l_orderkey=..., o_orderdate=..., o_shippriority=..., revenue=..."
        // 你可以自定义格式化
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
    public void close() {
        System.out.println("=== Q3 最终快照结果 ===");
        groupRevenue.entrySet().stream()
//                .filter(e -> Math.abs(e.getValue()) > 1e-8)
                .sorted((a, b) -> Double.compare(b.getValue(), a.getValue())) // 按revenue降序
                .forEach(e -> System.out.println(e.getKey() + ", revenue=" + e.getValue()));
        System.out.println("=====================");
    }
}