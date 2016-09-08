package com.ares.hadoop.mr.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Long, String, String, Long --> LongWritable, Text, Text, LongWritable
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	private final static LongWritable ONE = new LongWritable(1L) ;
	private Text word = new Text();
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.map(key, value, context);
		StringTokenizer itr = new StringTokenizer(value.toString(), " ");
		while (itr.hasMoreTokens()) {
			//efficiency is not well
			//context.write(new Text(itr.nextToken()), new LongWritable(1L));
			word.set(itr.nextToken());
			context.write(word, ONE);			
		}
	}
}
