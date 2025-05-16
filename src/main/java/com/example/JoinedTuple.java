package com.example;

import java.io.Serializable;

public class JoinedTuple implements Serializable {
    public String orderkey;        // l_orderkey
    public String orderdate;       // o_orderdate
    public String shippriority;    // o_shippriority
    public double extendedprice;   // l_extendedprice
    public double discount;        // l_discount
    public String opType;          // "INSERT" or "DELETE"

    public JoinedTuple(Lineitem li, Orders o, String opType) {
        this.orderkey = li.orderkey;
        this.orderdate = o.orderdate;
        this.shippriority = o.shippriority;
        this.extendedprice = parseDoubleSafe(li.extendedprice);
        this.discount = parseDoubleSafe(li.discount);
        this.opType = opType;
    }

    // 如果有需要也可以加另一个构造方法
    public JoinedTuple(String orderkey, String orderdate, String shippriority, double extendedprice, double discount, String opType) {
        this.orderkey = orderkey;
        this.orderdate = orderdate;
        this.shippriority = shippriority;
        this.extendedprice = extendedprice;
        this.discount = discount;
        this.opType = opType;
    }

    private double parseDoubleSafe(String str) {
        try {
            return Double.parseDouble(str.trim());
        } catch (Exception e) {
            return 0.0;
        }
    }

    @Override
    public String toString() {
        return "JoinedTuple{" +
                "orderkey='" + orderkey + '\'' +
                ", orderdate='" + orderdate + '\'' +
                ", shippriority='" + shippriority + '\'' +
                ", extendedprice=" + extendedprice +
                ", discount=" + discount +
                ", opType='" + opType + '\'' +
                '}';
    }
}
