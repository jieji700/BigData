package com.jiang.flowcount;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

class FlowBean implements Writable {
    //private values
    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public FlowBean() {
        super();
    }

    public FlowBean(long upFlow, long downFlow) {
        super();
        this.setUpFlow(upFlow);
        this.setDownFlow(downFlow);
        this.setSumFlow(upFlow + downFlow);
    }

    //serializable
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    //unserializable
    public void readFields(DataInput dataInput) throws IOException {
        setUpFlow(dataInput.readLong());
        setDownFlow(dataInput.readLong());
        setSumFlow(dataInput.readLong());
    }

    @Override
    public String toString() {
        return getUpFlow() + "\t" + getDownFlow() + "\t" + getSumFlow();
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void set(long upFlow, long downFlow) {
        this.setUpFlow(upFlow);
        this.setDownFlow(downFlow);
        this.setSumFlow(upFlow + downFlow);
    }
}
