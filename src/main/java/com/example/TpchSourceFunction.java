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
                if (line.startsWith("+LI")) {
                    String[] fields = line.substring(1).split("\\|", -1);
                    if (fields.length >= 17) {
                        Lineitem li = new Lineitem(fields);
                        ctx.collect(Tuple2.of("lineitem", li));
                    }
                } else if (line.startsWith("+OR")) {
                    String[] fields = line.substring(1).split("\\|", -1);
                    if (fields.length >= 10) {
                        Orders or = new Orders(fields);
                        ctx.collect(Tuple2.of("orders", or));
                    }
                } else if (line.startsWith("+CU")) {
                    String[] fields = line.substring(1).split("\\|", -1);
                    if (fields.length >= 9) {
                        Customer cu = new Customer(fields);
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