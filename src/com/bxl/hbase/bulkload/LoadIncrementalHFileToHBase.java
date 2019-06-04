package com.bxl.hbase.bulkload;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnectable;

import java.sql.Connection;

/**
 * Created by root on 2018/3/4.
 */
public class LoadIncrementalHFileToHBase {

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        HConnection conn = HConnectionManager.createConnection(conf);
        LoadIncrementalHFiles loder = new LoadIncrementalHFiles(conf);
        loder.doBulkLoad(new Path("/user/iteblog/output"),new HTable(conf,"blog_info"));
        System.out.println("成功了！！！");
    }
}
