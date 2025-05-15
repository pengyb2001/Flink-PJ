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
    public String opType; // "INSERT" or "DELETE"

    public Lineitem(String[] fields, String opType) {
        // fields[0]已经只保留数字
        this.orderkey = fields[0];
        this.partkey = fields[1];
        this.suppkey = fields[2];
        this.linenumber = fields[3];
        this.quantity = fields[4];
        this.extendedprice = fields[5];
        this.discount = fields[6];
        this.tax = fields[7];
        this.returnflag = fields[8];
        this.linestatus = fields[9];
        this.shipdate = fields[10];
        this.commitdate = fields[11];
        this.receiptdate = fields[12];
        this.shipinstruct = fields[13];
        this.shipmode = fields[14];
        this.comment = fields[15];
        this.opType = opType;
    }

    @Override
    public String toString() {
        return "Lineitem{" +
                "orderkey='" + orderkey + '\'' +
                ", extendedprice='" + extendedprice + '\'' +
                ", discount='" + discount + '\'' +
                ", shipdate='" + shipdate + '\'' +
                ", opType='" + opType + '\'' +
                '}';
    }
}