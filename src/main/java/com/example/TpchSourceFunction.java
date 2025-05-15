package com.example;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.FileReader;

public class TpchSourceFunction implements SourceFunction<Tuple2<String, Object>> {
    private volatile boolean isRunning = true;
    private final String filePath;

    public TpchSourceFunction(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void run(SourceContext<Tuple2<String, Object>> ctx) throws Exception {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while (isRunning && (line = br.readLine()) != null) {
                if (line.length() < 3) continue;
                String opType = line.startsWith("+") ? "INSERT" : (line.startsWith("-") ? "DELETE" : "UNKNOWN");
                // 剔除首位符号
                String content = line.substring(1);
                if (content.startsWith("LI")) {
                    String[] fields = content.substring(2).split("\\|", -1);
                    // 提取 key，只保留数字（orderkey）
                    if (fields.length >= 16) {
                        fields[0] = fields[0].replaceAll("[^0-9]", "");
                        Lineitem li = new Lineitem(fields, opType);
                        ctx.collect(Tuple2.of("lineitem", li));
                    }
                } else if (content.startsWith("OR")) {
                    String[] fields = content.substring(2).split("\\|", -1);
                    if (fields.length >= 9) {
                        fields[0] = fields[0].replaceAll("[^0-9]", "");
                        Orders or = new Orders(fields, opType);
                        ctx.collect(Tuple2.of("orders", or));
                    }
                } else if (content.startsWith("CU")) {
                    String[] fields = content.substring(2).split("\\|", -1);
                    if (fields.length >= 8) {
                        fields[0] = fields[0].replaceAll("[^0-9]", "");
                        Customer cu = new Customer(fields, opType);
                        ctx.collect(Tuple2.of("customer", cu));
                    }
                }
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}