package com.example;

public class Customer {
    public String custkey;
    public String name;
    public String address;
    public String nationkey;
    public String phone;
    public String acctbal;
    public String mktsegment;
    public String comment;
    public String opType; // "INSERT" or "DELETE"

    public Customer(String[] fields, String opType) {
        // fields[0]已经只保留数字
        this.custkey = fields[0];
        this.name = fields[1];
        this.address = fields[2];
        this.nationkey = fields[3];
        this.phone = fields[4];
        this.acctbal = fields[5];
        this.mktsegment = fields[6];
        this.comment = fields[7];
        this.opType = opType;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "custkey='" + custkey + '\'' +
                ", mktsegment='" + mktsegment + '\'' +
                ", opType='" + opType + '\'' +
                '}';
    }
}