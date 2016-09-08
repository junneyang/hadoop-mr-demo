package com.ares.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class MRTest {
	private static final Logger LOGGER = Logger.getLogger(MRTest.class);
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		LOGGER.debug("MRTest: MRTest STARTED...");
		
		if (args.length != 2) {
			LOGGER.error("MRTest: ARGUMENTS ERROR");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		//FOR Eclipse JVM Debug  
		//conf.set("mapreduce.job.jar", "wordcount.jar");
		Job job = Job.getInstance(conf);
		
		// JOB NAME
		job.setJobName("wordcount");
		
		// JOB MAPPER & REDUCER
		job.setJarByClass(MRTest.class);
		job.setMapperClass(WordCountMapper.class);
		// Combiner
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		
		// MAP & REDUCE
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		// MAP
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		// JOB INPUT & OUTPUT PATH
		//FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.setInputPaths(job, args[0]);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		// VERBOSE OUTPUT
		if (job.waitForCompletion(true)) {
			LOGGER.debug("MRTest: MRTest SUCCESSFULLY...");
		} else {
			LOGGER.debug("MRTest: MRTest FAILED...");
		}
		
		LOGGER.debug("MRTest: MRTest COMPLETED...");
	}
}
