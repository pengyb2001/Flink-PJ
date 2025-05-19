package com.example.util;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.time.LocalDate;

/**
 * LocalDate序列化器
 * 用于在Flink中序列化和反序列化LocalDate对象
 */
public class LocalDateSerializer extends Serializer<LocalDate> {
    
    /**
     * 序列化LocalDate对象
     * 
     * @param kryo Kryo实例
     * @param output 输出流
     * @param date 要序列化的LocalDate对象
     */
    @Override
    public void write(Kryo kryo, Output output, LocalDate date) {
        output.writeInt(date.getYear());      // 写入年份
        output.writeInt(date.getMonthValue()); // 写入月份
        output.writeInt(date.getDayOfMonth()); // 写入日期
    }

    /**
     * 反序列化LocalDate对象
     * 
     * @param kryo Kryo实例
     * @param input 输入流
     * @param type LocalDate类型
     * @return 反序列化后的LocalDate对象
     */
    @Override
    public LocalDate read(Kryo kryo, Input input, Class<LocalDate> type) {
        int year = input.readInt();  // 读取年份
        int month = input.readInt(); // 读取月份
        int day = input.readInt();   // 读取日期
        return LocalDate.of(year, month, day);
    }
} 