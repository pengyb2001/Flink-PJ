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

    public Customer() {}

    public Customer(String[] fields) {
        // fields[0] = CU1
        this.custkey = fields[1];
        this.name = fields[2];
        this.address = fields[3];
        this.nationkey = fields[4];
        this.phone = fields[5];
        this.acctbal = fields[6];
        this.mktsegment = fields[7];
        this.comment = fields[8];
    }

    @Override
    public String toString() {
        return "Customer{" +
                "custkey='" + custkey + '\'' +
                ", mktsegment='" + mktsegment + '\'' +
                ", name='" + name + '\'' +
                '}';
    }
}
