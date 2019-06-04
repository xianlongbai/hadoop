package com.bxl.mapreduce.tfidf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 这个job需在集群中云行
 */
public class LastJob {

	private static void lastJob(String in,String out,String cache1,String cache2){
		Configuration config = new Configuration(true);
		config.set("mapreduce.app-submission.cross-platform", "true");
//		config.set("mapred.jar", "D:\\tmp\\weibo3.jar");
		config.set("mapreduce.framework.name", "local");
		config.set("dfs.blocksize", "5242880");  //设置block块大小为5m
		try {
			FileSystem fs = FileSystem.get(config);

			Job job = Job.getInstance(config);
			job.setJarByClass(LastJob.class);
			job.setJobName("weibo3");
			job.setJar("D:\\tmp\\weibo3.jar");
			// 2.5
			//这里只能在集群中运行,但缓存的是本地文件
			// 把微博总数加载到内存
			//weibo_01/part-r-00003    weibo_02/part-r-00000
			job.addCacheFile(new Path(cache1).toUri());

			// 把df加载到内存
			job.addCacheFile(new Path(cache2).toUri());

			// 设置map任务的输出key类型、value类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			job.setMapperClass(LastMapper.class);
			job.setReducerClass(LastReduce.class);

			// mr运行时的输入数据从hdfs的哪个目录中获取
			FileInputFormat.addInputPath(job, new Path(in));
			Path outpath = new Path(out);
			if (fs.exists(outpath)) {
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job, outpath);

			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println("执行job成功");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}



	public static void main(String[] args) {
		args = new String[4];

		//提交集群
		args[0] = "/user/root/bxl/weibo_01";
		args[1] = "/user/root/bxl/weibo_03";
		args[2] = "/user/root/bxl/weibo_01/part-r-00003";
		args[3] = "/user/root/bxl/weibo_02/part-r-00000";
		lastJob(args[0],args[1],args[2],args[3]);

		//本地(会报错)
//		args[0] = "D:\\tmp\\intest\\tfidf\\weibo_01";
//		args[1] = "D:\\tmp\\intest\\tfidf\\weibo_03";
//		args[2] = "file:/D:\\tmp\\intest\\tfidf\\weibo_01\\part-r-00003";
//		args[3] = "file:/D:\\tmp\\intest\\tfidf\\weibo_02\\part-r-00000";
//		lastJob(args[0],args[1],args[2],args[3]);
	}
}
