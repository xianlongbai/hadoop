package com.bxl.mapreduce.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Freduce  extends Reducer<Text, IntWritable, Text, IntWritable>{

	//Text mkey = new Text();
	IntWritable val = new IntWritable();
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> vals,
			Context context) throws IOException, InterruptedException {
		/**
		 *穿入的数据格式
		 * hello_world  0
		 * hadoop_hello 1
		 */

		int sum = 0;
		for (IntWritable val : vals) {
			if(val.get()==0){
				break;
			}else{
				sum += val.get();
			}
		}
		if(sum != 0){
			val.set(sum);
			context.write(key,val);
		}
	}
}
