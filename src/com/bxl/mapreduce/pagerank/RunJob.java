package com.bxl.mapreduce.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {

	public static enum Mycounter {
		//枚举
		my
	}

	public static void main(String[] args) {
		Configuration config = new Configuration(false);
		
		config.set("mapreduce.framework.name", "local");
		config.set("mapreduce.app-submission.cross-platform", "true");

//		config.set("mapred.jar", "D://MR/pagerank.jar");
		
		// 收敛值
		double d = 0.001;
		int i = 0;
		while (true) {
			i++;
			try {
				// 记录计算的次数
				config.setInt("runCount", i);

				FileSystem fs = FileSystem.get(config);
				Job job = Job.getInstance(config);

				job.setJarByClass(RunJob.class);
				job.setJobName("pr" + i);
				job.setInputFormatClass(KeyValueTextInputFormat.class);  //设置输入的K_V格式（输入格式化类）
				job.setMapperClass(PageRankMapper.class);
				job.setReducerClass(PageRankReducer.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);

				Path inputPath = new Path("D:\\tmp\\intest\\pagerank.txt");

				if (i > 1) {
					//迭代计算输入的数据为上一次输出的数据
					inputPath = new Path("D:\\tmp\\intest\\pagerank_out\\pr" + (i - 1));
				}
				FileInputFormat.addInputPath(job, inputPath);

				Path outpath = new Path("D:\\tmp\\intest\\pagerank_out\\pr" + i);
				if (fs.exists(outpath)) {
					fs.delete(outpath, true);
				}
				FileOutputFormat.setOutputPath(job, outpath);

				//将reduce输出文件压缩
				FileOutputFormat.setCompressOutput(job, true);  //job使用压缩
				FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); //设置压缩格式
//				FileOutputFormat.setOutputCompressorClass(job, Lz4Codec.class); //设置压缩格式
				FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class); //设置压缩格式
//				FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class); //设置压缩格式

				boolean f = job.waitForCompletion(true);
				if (f) {
					System.out.println("success.");
					// 获取 计数器 中的差值
					//MR扩展，计数器
					long sum = job.getCounters().findCounter(Mycounter.my).getValue();

					System.out.println("SUM:  " + sum);
					double avgd = sum / 4000.0;
					if (avgd < d) {
						break;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	static class PageRankMapper extends Mapper<Text, Text, Text, Text> {
		@Override
		protected void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			int runCount = context.getConfiguration().getInt("runCount", 1);
			// A	B	D
			String page = key.toString();
			Node node = null;
			if (runCount == 1) { //第一次计算 初始化PR值为1.0
				node = Node.fromMR("1.0" + "\t" + value.toString());
			} else {
				node = Node.fromMR(value.toString());
			}
			// A:1.0 B D
			// 将计算前的数据输出  reduce计算做差值
			context.write(new Text(page), new Text(node.toString()));

			if (node.containsAdjacentNodes()) {
				double outValue = node.getPageRank() / node.getAdjacentNodeNames().length;
				for (int i = 0; i < node.getAdjacentNodeNames().length; i++) {
					String outPage = node.getAdjacentNodeNames()[i];
					// B:0.5
					// D:0.5
					context.write(new Text(outPage), new Text(outValue + ""));
				}
			}
		}
	}

	static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {
			double sum = 0.0;
			Node sourceNode = null;
			for (Text i : iterable) {
				Node node = Node.fromMR(i.toString());
				//A:1.0 B D
				if (node.containsAdjacentNodes()) {
					// 计算前的数据 // A:1.0 B D
					sourceNode = node;
				} else {
					// B:0.5 // D:0.5
					sum = sum + node.getPageRank();
				}
			}

			// 计算新的PR值  4为页面总数
			double newPR = (0.15 / 4.0) + (0.85 * sum);
			System.out.println("*********** new pageRank value is " + newPR);

			// 把新的pr值和计算之前的pr比较
			double d = newPR - sourceNode.getPageRank();

			int j = (int) (d * 1000.0);
			j = Math.abs(j);
			System.out.println(j + "___________");
			// 累加
			context.getCounter(Mycounter.my).increment(j);

			sourceNode.setPageRank(newPR);
			context.write(key, new Text(sourceNode.toString()));
		}
	}
}
