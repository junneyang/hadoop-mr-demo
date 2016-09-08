package com.ares.hadoop.mr.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>{
	private LongWritable result = new LongWritable();
	
	@Override
	protected void reduce(Text key, Iterable<LongWritable> vlaues,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		//super.reduce(arg0, arg1, arg2);
		long sum = 0;
		for (LongWritable value : vlaues) {
			sum += value.get();
		}
		result.set(sum);
		context.write(key, result);
	}
}
