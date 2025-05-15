package com.example;

public class Orders {
    public String orderkey;
    public String custkey;
    public String orderstatus;
    public String totalprice;
    public String orderdate;
    public String orderpriority;
    public String clerk;
    public String shippriority;
    public String comment;
    public String opType; // "INSERT" or "DELETE"

    public Orders(String[] fields, String opType) {
        // fields[0]已经只保留数字
        this.orderkey = fields[0];
        this.custkey = fields[1];
        this.orderstatus = fields[2];
        this.totalprice = fields[3];
        this.orderdate = fields[4];
        this.orderpriority = fields[5];
        this.clerk = fields[6];
        this.shippriority = fields[7];
        this.comment = fields[8];
        this.opType = opType;
    }

    @Override
    public String toString() {
        return "Orders{" +
                "orderkey='" + orderkey + '\'' +
                ", custkey='" + custkey + '\'' +
                ", orderdate='" + orderdate + '\'' +
                ", opType='" + opType + '\'' +
                '}';
    }
}