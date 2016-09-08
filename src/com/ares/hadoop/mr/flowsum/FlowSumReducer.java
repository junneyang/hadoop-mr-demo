package com.ares.hadoop.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlowSumReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
	//private static final Logger LOGGER = Logger.getLogger(MRTest.class);
	
	private FlowBean flowBean = new FlowBean();
	
	@Override
	protected void reduce(Text key, Iterable<FlowBean> values,
			Reducer<Text, FlowBean, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		long upFlowCounter = 0;
		long downFlowCounter = 0;
		
		for (FlowBean flowBean : values) {
			upFlowCounter += flowBean.getUpFlow();
			downFlowCounter += flowBean.getDownFlow();
		}
		flowBean.setPhoneNum(key.toString());
		flowBean.setUpFlow(upFlowCounter);
		flowBean.setDownFlow(downFlowCounter);
		flowBean.setSumFlow(upFlowCounter + downFlowCounter);
		
		context.write(key, flowBean);
	}
}
