package com.ares.hadoop.mr.flowgroup;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import com.ares.hadoop.mr.exception.LineException;

public class FlowSort extends Configured implements Tool {
	private static final Logger LOGGER = Logger.getLogger(FlowGroup.class);
	enum Counter {
		LINESKIP
	}
	
	public static class FlowSortMapper extends Mapper<LongWritable, Text, 
		FlowBean, NullWritable> {
		private String line;
		private int length;
		private final static char separator = '\t';
		
		private String phoneNum;
		private long upFlow;
		private long downFlow;
		private long sumFlow;
		
		private FlowBean flowBean = new FlowBean();
		private NullWritable nullWritable = NullWritable.get();
		
		@Override
		protected void map(
				LongWritable key,
				Text value,
				Mapper<LongWritable, Text, FlowBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.map(key, value, context);
			String errMsg;
			try {
				line = value.toString();
				String[] fields = StringUtils.split(line, separator);
				length = fields.length;
				if (length != 4) {
					throw new LineException(key.get() + ", " + line + " LENGTH INVALID, IGNORE...");
				}
				
				phoneNum = fields[0];
				upFlow = Long.parseLong(fields[1]);
				downFlow = Long.parseLong(fields[2]);
				sumFlow = Long.parseLong(fields[3]);
				
				flowBean.setPhoneNum(phoneNum);
				flowBean.setUpFlow(upFlow);
				flowBean.setDownFlow(downFlow);
				flowBean.setSumFlow(sumFlow);
				
				context.write(flowBean, nullWritable);
			} catch (LineException e) {
				// TODO: handle exception
				LOGGER.error(e);
				System.out.println(e);
				context.getCounter(Counter.LINESKIP).increment(1);
				return;
			} catch (NumberFormatException e) {
				// TODO: handle exception
				errMsg = key.get() + ", " + line + " FLOW DATA INVALID, IGNORE...";
				LOGGER.error(errMsg);
				System.out.println(errMsg);
				context.getCounter(Counter.LINESKIP).increment(1);
				return;
			} catch (Exception e) {
				// TODO: handle exception
				LOGGER.error(e);
				System.out.println(e);
				context.getCounter(Counter.LINESKIP).increment(1);
				return;
			}			
		}
	}
	
	public static class FlowSortReducer extends Reducer<FlowBean, NullWritable, 
		FlowBean, NullWritable> {
		@Override
		protected void reduce(
				FlowBean key,
				Iterable<NullWritable> values,
				Reducer<FlowBean, NullWritable, FlowBean, NullWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.reduce(arg0, arg1, arg2);
			context.write(key, NullWritable.get());
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		String errMsg = "FlowSort: TEST STARTED...";
		LOGGER.debug(errMsg);
		System.out.println(errMsg);
		
		Configuration conf = new Configuration();
		//FOR Eclipse JVM Debug  
		//conf.set("mapreduce.job.jar", "flowsum.jar");
		Job job = Job.getInstance(conf);
		
		// JOB NAME
		job.setJobName("FlowSort");
		
		// JOB MAPPER & REDUCER
		job.setJarByClass(FlowGroup.class);
		job.setMapperClass(FlowSortMapper.class);
		job.setReducerClass(FlowSortReducer.class);
		
		// MAP & REDUCE
		job.setOutputKeyClass(FlowBean.class);
		job.setOutputValueClass(NullWritable.class);
		// MAP
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		// JOB INPUT & OUTPUT PATH
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.setInputPaths(job, args[1]);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		// VERBOSE OUTPUT
		if (job.waitForCompletion(true)) {
			errMsg = "FlowSort: TEST SUCCESSFULLY...";
			LOGGER.debug(errMsg);
			System.out.println(errMsg);
			return 0;
		} else {
			errMsg = "FlowSort: TEST FAILED...";
			LOGGER.debug(errMsg);
			System.out.println(errMsg);
			return 1;
		}			
		
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			String errMsg = "FlowSort: ARGUMENTS ERROR";
			LOGGER.error(errMsg);
			System.out.println(errMsg);
			System.exit(-1);
		}
		
		int result = ToolRunner.run(new Configuration(), new FlowGroup(), args);
		System.exit(result);
	}
}
