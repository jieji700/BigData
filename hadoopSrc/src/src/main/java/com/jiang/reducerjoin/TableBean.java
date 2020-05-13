package com.jiang.reducerjoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable {
    public void setFlag(String flag) {
        this.flag = flag;
    }

    private String order_id;//订单id
    private String p_id;//产品id
    private int amount;//产品数量
    private String pname;//产品名称
    private String flag;//表的标记

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(order_id);
        dataOutput.writeUTF(p_id);
        dataOutput.writeInt(amount);
        dataOutput.writeUTF(pname);
        dataOutput.writeUTF(flag);
    }

    @Override
    public String toString() {
        return "TableBean{" +
                "order_id='" + order_id + '\'' +
                ", amount=" + amount +
                ", pname='" + pname + '\'' +
                '}';
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.setOrder_id(dataInput.readUTF());
        this.setP_id(dataInput.readUTF());
        this.setAmount(dataInput.readInt());
        this.setPname(dataInput.readUTF());
        this.setFlag(dataInput.readUTF());
    }

    public TableBean() {
        super();
    }

    public TableBean(String order_id, String p_id, int amount, String pname, String flag) {
        super();
        this.order_id = order_id;
        this.p_id = p_id;
        this.amount = amount;
        this.pname = pname;
        this.flag = flag;
    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getP_id() {
        return p_id;
    }

    public void setP_id(String p_id) {
        this.p_id = p_id;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

}
