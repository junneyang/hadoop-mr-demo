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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class InverseIndexOne extends Configured implements Tool {
	
	private static final Logger LOGGER = Logger.getLogger(InverseIndexOne.class);
	enum Counter {
		LINESKIP
	}
	
	public static class InverseIndexOneMapper 
		extends Mapper<LongWritable, Text, Text, LongWritable> {
		
		private String line;
		private final static char separatorA = ' ';
		private final static char separatorB = '-';
		private String fileName;
		
		private Text text = new Text();
		private final static LongWritable ONE = new LongWritable(1L);
		
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.map(key, value, context);
			try {
				line = value.toString();
				String[] fields = StringUtils.split(line, separatorA);
				
				FileSplit fileSplit = (FileSplit) context.getInputSplit();
				fileName = fileSplit.getPath().getName();
				
				for (int i = 0; i < fields.length; i++) {
					text.set(fields[i] + separatorB + fileName);
					context.write(text, ONE);
				}
			} catch (Exception e) {
				// TODO: handle exception
				LOGGER.error(e);
				System.out.println(e);
				context.getCounter(Counter.LINESKIP).increment(1);
				return;
			}			
		}
	}
	
	public static class InverseIndexOneReducer 
	extends Reducer<Text, LongWritable, Text, LongWritable> {
		private LongWritable result = new LongWritable();
		
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values,
				Reducer<Text, LongWritable, Text, LongWritable>.Context context)
				throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			//super.reduce(arg0, arg1, arg2);
			long count = 0;
			for (LongWritable value : values) {
				count += value.get();
			}
			result.set(count);
			context.write(key, result);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		//return 0;
		String errMsg = "InverseIndexOne: TEST STARTED...";
		LOGGER.debug(errMsg);
		System.out.println(errMsg);
		
		Configuration conf = new Configuration();
		//FOR Eclipse JVM Debug  
		//conf.set("mapreduce.job.jar", "flowsum.jar");
		Job job = Job.getInstance(conf);
		
		// JOB NAME
		job.setJobName("InverseIndexOne");
		
		// JOB MAPPER & REDUCER
		job.setJarByClass(InverseIndexOne.class);
		job.setMapperClass(InverseIndexOneMapper.class);
		job.setReducerClass(InverseIndexOneReducer.class);
		
		// JOB PARTITION
		//job.setPartitionerClass(FlowGroupPartition.class);
		
		// JOB REDUCE TASK NUMBER
		//job.setNumReduceTasks(5);
		
		// MAP & REDUCE
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		// MAP
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
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
			errMsg = "InverseIndexOne: TEST SUCCESSFULLY...";
			LOGGER.debug(errMsg);
			System.out.println(errMsg);
			return 0;
		} else {
			errMsg = "InverseIndexOne: TEST FAILED...";
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
		
		int result = ToolRunner.run(new Configuration(), new InverseIndexOne(), args);
		System.exit(result);
	}
}
