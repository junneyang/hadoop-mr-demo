package com.ares.hadoop.mr.inverseindex;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
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

public class InverseIndexTwo extends Configured implements Tool{
	private static final Logger LOGGER = Logger.getLogger(InverseIndexOne.class);
	enum Counter {
		LINESKIP
	}
	
	public static class InverseIndexTwoMapper extends 
	Mapper<LongWritable, Text, Text, Text> {
		
		private String line;
		private final static char separatorA = '\t';
		private final static char separatorB = '-';		
		
		private Text textKey = new Text();
		private Text textValue = new Text();
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.map(key, value, context);
			try {
				line = value.toString();
				String[] fields = StringUtils.split(line, separatorA);
				String[] wordAndfileName = StringUtils.split(fields[0], separatorB);
				long count = Long.parseLong(fields[1]);
				String word = wordAndfileName[0];
				String fileName = wordAndfileName[1];
				
				textKey.set(word);
				textValue.set(fileName + separatorB + count);
				context.write(textKey, textValue);
			} catch (Exception e) {
				// TODO: handle exception
				LOGGER.error(e);
				System.out.println(e);
				context.getCounter(Counter.LINESKIP).increment(1);
				return;
			}			
		}
	}
	
	public static class InverseIndexTwoReducer extends 
	Reducer<Text, Text, Text, Text> {
		
		private Text textValue = new Text();
		
		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.reduce(arg0, arg1, arg2);
			StringBuilder index = new StringBuilder("");
//			for (Text text : values) {
//				if (condition) {
//					
//				}
//				index.append(text.toString() + separatorA);
//			}
			String separatorA = "";
			for (Text text : values) {
				index.append(separatorA + text.toString());
				separatorA = ",";
			}
			textValue.set(index.toString());
			context.write(key, textValue);
		}
	}
	
	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//return 0;
		String errMsg = "InverseIndexTwo: TEST STARTED...";
		LOGGER.debug(errMsg);
		System.out.println(errMsg);
		
		Configuration conf = new Configuration();
		//FOR Eclipse JVM Debug  
		//conf.set("mapreduce.job.jar", "flowsum.jar");
		Job job = Job.getInstance(conf);
		
		// JOB NAME
		job.setJobName("InverseIndexTwo");
		
		// JOB MAPPER & REDUCER
		job.setJarByClass(InverseIndexTwo.class);
		job.setMapperClass(InverseIndexTwoMapper.class);
		job.setReducerClass(InverseIndexTwoReducer.class);
		
		// JOB PARTITION
		//job.setPartitionerClass(FlowGroupPartition.class);
		
		// JOB REDUCE TASK NUMBER
		//job.setNumReduceTasks(5);
		
		// MAP & REDUCE
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// MAP
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		// JOB INPUT & OUTPUT PATH
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.setInputPaths(job, args[1]);
		Path output = new Path(args[2]);
//		FileSystem fs = FileSystem.get(conf);
//		if (fs.exists(output)) {
//			fs.delete(output, true);
//		}
		FileOutputFormat.setOutputPath(job, output);
		
		// VERBOSE OUTPUT
		if (job.waitForCompletion(true)) {
			errMsg = "InverseIndexTwo: TEST SUCCESSFULLY...";
			LOGGER.debug(errMsg);
			System.out.println(errMsg);
			return 0;
		} else {
			errMsg = "InverseIndexTwo: TEST FAILED...";
			LOGGER.debug(errMsg);
			System.out.println(errMsg);
			return 1;
		}			
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			String errMsg = "InverseIndexOne: ARGUMENTS ERROR";
			LOGGER.error(errMsg);
			System.out.println(errMsg);
			System.exit(-1);
		}
		
		int result = ToolRunner.run(new Configuration(), new InverseIndexTwo(), args);
		System.exit(result);
	}

}
