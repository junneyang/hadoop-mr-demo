package com.ares.hadoop.mr.flowgroup;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean>{
	private String phoneNum;
	private long upFlow;
	private long downFlow;
	private long sumFlow;
	
	public FlowBean() {
		// TODO Auto-generated constructor stub
	}	
//	public FlowBean(String phoneNum, long upFlow, long downFlow, long sumFlow) {
//		super();
//		this.phoneNum = phoneNum;
//		this.upFlow = upFlow;
//		this.downFlow = downFlow;
//		this.sumFlow = sumFlow;
//	}


	public String getPhoneNum() {
		return phoneNum;
	}

	public void setPhoneNum(String phoneNum) {
		this.phoneNum = phoneNum;
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

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		phoneNum = in.readUTF();
		upFlow = in.readLong();
		downFlow = in.readLong();
		sumFlow = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeUTF(phoneNum);
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	@Override
	public String toString() {
		return "" + phoneNum + "\t" + upFlow + "\t" + downFlow + "\t" + sumFlow;
	}

	@Override
	public int compareTo(FlowBean o) {
		// TODO Auto-generated method stub
		return sumFlow>o.getSumFlow()?-1:1;
	}
	
}
