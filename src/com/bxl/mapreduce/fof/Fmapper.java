package com.bxl.mapreduce.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author root
 *map的默认输入key不能为IntWritable
 */
public class Fmapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	Text mkey = new Text();
	IntWritable val = new IntWritable();

	//tom hello hadoop cat
	//输入1行数据
	//迭代后两两输出
	//标注关系（直接好友：0，间接好友：1）
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] strs = value.toString().split(" ");
		
		for(int i=0;i<strs.length;i++){
			String str = comp(strs[0],strs[i]);
			mkey.set(str);
			val.set(0);
			context.write(mkey, val);
			for(int j=i+1;j<strs.length-1;j++){  //便利到strs[3]会数组越界，所以lenth-1
				int temp = i+1;
				str = comp(strs[temp],strs[j+1]);
				mkey.set(str);
				val.set(1);
				context.write(mkey, val);
			}
		}
	}
	
	public static String comp(String s1,String s2){
		
		if (s1.compareTo(s2) > 0){
			return  s2+"_"+s1;
		}else{
			return s1+"_"+s2;
		}
	}
	
}
