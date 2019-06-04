package com.bxl.hbase.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import static org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2.configureIncrementalLoad;

import java.io.IOException;


/**
 * Created by root on 2018/3/4.
 * 1、首先打一个jar包放在本地
 * 2、以本地为client,提交任务到yarn上
 * 3、运行完成后会生成向HFile一样特定格式的文件
 * 4、最后将文件直接将数据文件加载到运行的集群中LoadIncrementalHFileToHBase
 * 5、这样导入数据的优点：占用更少的CPU和网络资源，且适合导入海量数据
 */
public class IteblogBulkLoadDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final String SRC_PATH= "/user/iteblog/input";
        final String DESC_PATH= "/user/iteblog/output";
        Configuration conf = HBaseConfiguration.create();
        conf.set("mapreduce.app-submission.cross-platform", "true");
//        Connection connection = ConnectionFactory.createConnection(conf);
        HConnection conn = HConnectionManager.createConnection(conf);
        Job job= Job.getInstance(conf);
        job.setJarByClass(IteblogBulkLoadDriver.class);
        job.setJar("D:\\tmp\\loadhbase.jar");
        job.setMapperClass(IteblogBulkLoadMapper.class);
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setOutputFormatClass(HFileOutputFormat2.class);
        HTable table = new HTable(conf,"blog_info"); //table.getRegionLocator()
//        HFileOutputFormat2.configureIncrementalLoad(job,table,table);
//        HFileOutputFormat.configureIncrementalLoad(job,table);
        HFileOutputFormat2.configureIncrementalLoad(job,table);
        FileInputFormat.addInputPath(job,new Path(SRC_PATH));
        FileOutputFormat.setOutputPath(job,new Path(DESC_PATH));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
