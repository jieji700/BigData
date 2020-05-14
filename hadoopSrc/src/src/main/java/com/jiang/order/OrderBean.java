package com.jiang.order;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {

    private int orderID;
    private double price;

    public OrderBean(int orderID, double price) {
        super();
        this.orderID = orderID;
        this.price = price;
    }

    public OrderBean() {
        super();
    }

    public int compareTo(OrderBean o) {
        int result;
        if (getOrderID() > o.getOrderID()) {
            result = 1;
        } else if (getOrderID() < o.getOrderID()) {
            result = -1;
        } else {
            if (getPrice() > o.getPrice()) {
                result = -1;
            } else if (getPrice() < o.getPrice()) {
                result = 1;
            } else {
                result = 0;
            }
        }
        return result;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(this.getOrderID());
        dataOutput.writeDouble(this.getPrice());
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.setOrderID(dataInput.readInt());
        this.setPrice(dataInput.readDouble());
    }

    public int getOrderID() {
        return orderID;
    }

    public void setOrderID(int orderID) {
        this.orderID = orderID;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderBean{" +
                "orderID=" + orderID +
                ", price=" + price +
                '}';
    }
}
