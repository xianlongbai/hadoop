package com.bxl.mapreduce.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by root on 2018/2/14.
 * 需求：计算出每个月温度最高的两天和对应的温度
 *	 1949-10-01 14:21:02	34c
     1949-10-01 19:21:02	38c
     1949-10-02 14:01:02	36c
     1950-01-01 11:21:02	32c
     1950-10-01 12:21:02	37c
     1951-12-01 12:21:02	23c
     1950-10-02 12:21:02	41c
     1950-10-03 12:21:02	27c
     1951-07-01 12:21:02	45c
     1951-07-02 12:21:02	46c
     1951-07-03 12:21:03	47c
 */

public class MyJob {


    private static void topN(String input,String output) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration(false);
        Job job = Job.getInstance(conf);
        conf.set("mapreduce.app-submission.cross-platform", "true");
        conf.set("mapreduce.framework.name", "local");
//        job.setJarByClass(MyJob.class);
        job.setJobName("hot_topN");

        job.setMapperClass(HotMapper.class);
        job.setMapOutputKeyClass(HotWeather.class);
        job.setMapOutputValueClass(IntWritable.class);
        //为了同时启用多个reduce，需要给输出数据进行打标签，就涉及到分区算法
        job.setNumReduceTasks(3);
        /**
         * 分区算法要保证相同的key去到一组
         * 目的是将不同的key分发到不同的reduce
         * 按 年 月 计算分区号
         * 类似分组比较器，可以收敛计算宽度
         * 例：按年计算分区号
         */
        job.setPartitionerClass(HotPartition.class);
        //自定义比较器，认为干预排序结果（倒序），不采用默认的排序,这时候就不会采用Hotweather默认比较器
        job.setSortComparatorClass(Hotsort.class);
        //自定义分组比较器，人为干预分组边界
        job.setGroupingComparatorClass(HotGroup.class);
        //定义reduce函数
        job.setReducerClass(HotReduce.class);

        //定义作业的输入输出路径
        Path inputPath = new Path(input);
        Path outputPath = new Path(output);
        FileInputFormat.addInputPath(job, inputPath);  //注意：别导错包import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        if(outputPath.getFileSystem(conf).exists(outputPath)){
            outputPath.getFileSystem(conf).delete(outputPath);
        }
        FileOutputFormat.setOutputPath(job, outputPath); //注意：别导错包import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

        //启动任务,并返回成功过或失败，0：成功，1：失败
        System.exit(job.waitForCompletion(true)?0:1);
    }


    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        topN("D:\\tmp\\tq.txt","D:\\tmp\\intest\\topN");
    }
}
