package com.example;

import java.util.Objects;

public class GroupKey {
    public String orderkey;
    public String orderdate;
    public String shippriority;

    public GroupKey(String orderkey, String orderdate, String shippriority) {
        this.orderkey = orderkey;
        this.orderdate = orderdate;
        this.shippriority = shippriority;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GroupKey)) return false;
        GroupKey groupKey = (GroupKey) o;
        return Objects.equals(orderkey, groupKey.orderkey) &&
                Objects.equals(orderdate, groupKey.orderdate) &&
                Objects.equals(shippriority, groupKey.shippriority);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderkey, orderdate, shippriority);
    }

    @Override
    public String toString() {
        return "GroupKey{" +
                "orderkey='" + orderkey + '\'' +
                ", orderdate='" + orderdate + '\'' +
                ", shippriority='" + shippriority + '\'' +
                '}';
    }
}