package com.atguigu.hdfsclient.Flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class FlowBean implements Writable {

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    @Override
    public String toString() {
        return "FlowBean{" +
                "upFlow=" + upFlow +
                ", downFlow=" + downFlow +
                ", sumFlow=" + sumFlow +
                '}';
    }

    public void set(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    public FlowBean() {
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

    /*
    序列化与反序列化的参数位置必须完全一致
     */

    /**
     * 序列化方法
     * @param dataOutput 框架提供的数据出口
     * @throws IOException
     */
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);

    }

    /**
     * 反序列化方法
     * @param dataInput 框架提供的数据来源
     * @throws IOException
     */
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }
}
