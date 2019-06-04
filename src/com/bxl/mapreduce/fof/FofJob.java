package com.bxl.mapreduce.fof;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




/**
 *
 * @author root
 *报错：Unable to initialize any output collector
 *一般是fjob.setOutputKeyClass(Text.class); 引用的包出现了问题
 *如：import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;
 */
public class FofJob {


	public static void fofJOb(String input,String output) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration(true);
		conf.set("dfs.blocksize", "5242880");  //设置block块大小为5m
		//除了GNU/Linux运行客户端都要为true
		conf.set("mapreduce.app-submission.cross-platform", "true");
		//本地运行，多线程模拟分布式集群
//		conf.set("mapreduce.framework.name", "local");
		//2、设置map输出的中间文件的压缩格式
		conf.setBoolean("mapred.compress.map.out", true);
		conf.setClass(Job.MAP_OUTPUT_COMPRESS_CODEC, BZip2Codec.class, CompressionCodec.class);
		//3-、设置reduce的压缩输出
		//conf.setBoolean("mapred.output.compress", true);//设置输出压缩
		Job fjob = Job.getInstance(conf);
		fjob.setJarByClass(FofJob.class);

//		JobConf jobConf = new JobConf(FofJob.class);
//		jobConf.setCompressMapOutput(true);
//		jobConf.setMapOutputCompressorClass(BZip2Codec.class);

		//这相当于将本地作为客户端cli,但依然将计算交由集群
		fjob.setJar("C:\\Users\\16050\\Desktop\\hadoop_tmp\\fof2.jar");
		fjob.setJobName("fofJOB_pkg");
		
		fjob.setMapperClass(Fmapper.class);
		fjob.setOutputKeyClass(Text.class);
		fjob.setOutputValueClass(IntWritable.class);
		fjob.setReducerClass(Freduce.class);

		FileInputFormat.addInputPath(fjob, new Path(input));
		Path outPath = new Path(output);
		if(outPath.getFileSystem(conf).exists(outPath)){
			outPath.getFileSystem(conf).delete(outPath);
		}
		FileOutputFormat.setOutputPath(fjob, outPath);
		//3、结果文件存为压缩文件
		FileOutputFormat.setCompressOutput(fjob, true);  //job使用压缩
		FileOutputFormat.setOutputCompressorClass(fjob, BZip2Codec.class); //设置压缩格式

		fjob.waitForCompletion(true);
		
	}
	
	
	
	
	
	
	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {
		//1、输入压缩格式
		String a ="/user/root/bxl/friend.bz2";
		String b ="/user/root/bxl/fof6";
		fofJOb(a, b);
	}
}
