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

    public Orders() {}

    public Orders(String[] fields) {
        // fields[0] = OR1
        this.orderkey = fields[1];
        this.custkey = fields[2];
        this.orderstatus = fields[3];
        this.totalprice = fields[4];
        this.orderdate = fields[5];
        this.orderpriority = fields[6];
        this.clerk = fields[7];
        this.shippriority = fields[8];
        this.comment = fields[9];
    }

    @Override
    public String toString() {
        return "Orders{" +
                "orderkey='" + orderkey + '\'' +
                ", custkey='" + custkey + '\'' +
                ", orderdate='" + orderdate + '\'' +
                '}';
    }
}