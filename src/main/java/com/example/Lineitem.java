package com.example;

public class Lineitem {
    public String orderkey;
    public String partkey;
    public String suppkey;
    public String linenumber;
    public String quantity;
    public String extendedprice;
    public String discount;
    public String tax;
    public String returnflag;
    public String linestatus;
    public String shipdate;
    public String commitdate;
    public String receiptdate;
    public String shipinstruct;
    public String shipmode;
    public String comment;

    public Lineitem() {}

    public Lineitem(String[] fields) {
        // fields下标从1开始，因为fields[0]是+LI1
        this.orderkey = fields[1];
        this.partkey = fields[2];
        this.suppkey = fields[3];
        this.linenumber = fields[4];
        this.quantity = fields[5];
        this.extendedprice = fields[6];
        this.discount = fields[7];
        this.tax = fields[8];
        this.returnflag = fields[9];
        this.linestatus = fields[10];
        this.shipdate = fields[11];
        this.commitdate = fields[12];
        this.receiptdate = fields[13];
        this.shipinstruct = fields[14];
        this.shipmode = fields[15];
        this.comment = fields[16];
    }

    @Override
    public String toString() {
        return "Lineitem{" +
                "orderkey='" + orderkey + '\'' +
                ", extendedprice='" + extendedprice + '\'' +
                ", discount='" + discount + '\'' +
                ", shipdate='" + shipdate + '\'' +
                '}';
    }
}