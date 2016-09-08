package com.ares.hadoop.mr.flowsum;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import com.ares.hadoop.mr.wordcount.MRTest;

//Long, String, String, Long --> LongWritable, Text, Text, LongWritable
public class FlowSumMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
	private static final Logger LOGGER = Logger.getLogger(MRTest.class);
	
	private String line;
	private int length;
	
	private String phoneNum;
	private long upFlow;
	private long downFlow;
	private long sumFlow;
	private FlowBean flowBean = new FlowBean();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, FlowBean>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context);
		line = value.toString();
		String[] fields = StringUtils.split(line);
		length = fields.length;
		if (length < 9) {
			LOGGER.error(key.get() + ", " + line + " INVALID, IGNORE...");
		}
		phoneNum = fields[1];
		upFlow = Long.parseLong(fields[length-3]);
		downFlow = Long.parseLong(fields[length-2]);
		sumFlow = upFlow + downFlow;
		flowBean.setPhoneNum(phoneNum);
		flowBean.setUpFlow(upFlow);
		flowBean.setDownFlow(downFlow);
		flowBean.setSumFlow(sumFlow);
	}
}
