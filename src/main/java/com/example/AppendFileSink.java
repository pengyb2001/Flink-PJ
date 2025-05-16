package com.example;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class AppendFileSink<T> implements SinkFunction<T> {
    private final String path;

    public AppendFileSink(String path) {
        this.path = path;
    }

    @Override
    public synchronized void invoke(T value, Context context) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path, true))) {
            writer.write(value.toString());
            writer.newLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}