package com.ares.hadoop.mr.flowgroup;

import java.util.HashMap;

import org.apache.hadoop.mapreduce.Partitioner;

public class FlowGroupPartition<KEY, VALUE> extends Partitioner<KEY, VALUE>{
	private static HashMap<String, Integer> groupMap = new HashMap<String, Integer>();
	static {
		groupMap.put("135", 0);
		groupMap.put("136", 1);
		groupMap.put("137", 2);
		groupMap.put("138", 3);
	}
	
	@Override
	public int getPartition(KEY key, VALUE value, int numPartitions) {
		// TODO Auto-generated method stub
		return (groupMap.get(key.toString().substring(0, 3)) == null)?4:
			groupMap.get(key.toString().substring(0, 3));
	}

}
