package com.ares.hadoop.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class FlowSumRunner extends Configured implements Tool {
	private static final Logger LOGGER = Logger.getLogger(FlowSumRunner.class);
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		LOGGER.debug("MRTest: MRTest STARTED...");
		
		if (args.length != 2) {
			LOGGER.error("MRTest: ARGUMENTS ERROR");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		//FOR Eclipse JVM Debug  
		//conf.set("mapreduce.job.jar", "flowsum.jar");
		Job job = Job.getInstance(conf);
		
		// JOB NAME
		job.setJobName("flowsum");
		
		// JOB MAPPER & REDUCER
		job.setJarByClass(FlowSumRunner.class);
		job.setMapperClass(FlowSumMapper.class);
		job.setReducerClass(FlowSumReducer.class);
		
		// MAP & REDUCE
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		// MAP
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		// JOB INPUT & OUTPUT PATH
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// VERBOSE OUTPUT
		if (job.waitForCompletion(true)) {
			LOGGER.debug("MRTest: MRTest SUCCESSFULLY...");
			return 0;
		} else {
			LOGGER.debug("MRTest: MRTest FAILED...");
			return 1;
		}			
		
	}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new FlowSumRunner(), args);
		System.exit(result);
	}

}
