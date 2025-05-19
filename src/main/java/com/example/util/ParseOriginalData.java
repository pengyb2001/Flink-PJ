package com.example.util;

/**
 * 数据记录类
 * 用于表示从输入文件中读取的原始数据记录
 * 格式为：操作类型(+/-) + 表类型(LI/OR/CU) + 数据内容
 */
public class ParseOriginalData {
    private String type;       // 操作类型: "+" 表示添加记录, "-" 表示删除记录
    private String tableType;  // 表类型: "LI" 表示LineItem, "OR" 表示Order, "CU" 表示Customer
    private String data;       // 实际数据内容

    /**
     * 构造函数
     * 
     * @param type 操作类型
     * @param tableType 表类型
     * @param data 数据内容
     */
    public ParseOriginalData(String type, String tableType, String data) {
        this.type = type;
        this.tableType = tableType;
        this.data = data;
    }

    /**
     * 获取操作类型
     * 
     * @return 操作类型 "+" 或 "-"
     */
    public String getType() {
        return type;
    }

    /**
     * 获取表类型
     * 
     * @return 表类型 "LI", "OR" 或 "CU"
     */
    public String getTableType() {
        return tableType;
    }

    /**
     * 获取数据内容
     * 
     * @return 数据内容字符串
     */
    public String getData() {
        return data;
    }

    /**
     * 从输入行创建DataRecord对象
     * 
     * @param line 输入行，格式为：操作类型 + 表类型 + 数据内容
     * @return 创建的DataRecord对象
     */
    public static ParseOriginalData fromString(String line) {
        String type = line.substring(0, 1);      // 第一个字符表示操作类型
        String tableType = line.substring(1, 3); // 第2-3个字符表示表类型
        String data = line.substring(3);         // 从第4个字符开始是数据内容
        return new ParseOriginalData(type, tableType, data);
    }
} 