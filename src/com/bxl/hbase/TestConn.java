package com.bxl.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
//import org.apache.hadoop.hbase.client.Connection;
//import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnectable;
/**
 * Created by root on 2018/3/4.
 */
public class TestConn {

    public static void main(String[] args) {

        // 连接
//        HConnectionManager factory = null;
        HConnectable hconn = null;
        HConnection conn = null;
        HTable messages = null;
        String TABLE_NAME = "test01";


        Configuration configuration = HBaseConfiguration.create();
//        configuration.set("hbase.zookeeper.property.clientPort", "2181");
//        configuration.set("hbase.zookeeper.quorum","192.168.1.21:2181,192.168.1.22:2181,192.168.1.23:2181");
//        configuration.set("hbase.master", "192.168.1.21:60000");



        try {
            //这种写法会报错：Exception in thread “main” java.io.IOException: The connection has to be unmanaged.
            //conn = HConnectionManager.getConnection(configuration);
            conn = HConnectionManager.createConnection(configuration);
            messages = (HTable) conn.getTable(TableName.valueOf(TABLE_NAME));
            System.out.println("连接到表"+messages);

            Get g = new Get("rk02".getBytes());
            Result r = messages.get(g);
            byte [] value = r.getValue(Bytes.toBytes("f2"), Bytes.toBytes("name"));
            String valueStr = Bytes.toString(value);

            System.out.println("Get r1 content: " + valueStr );

        } catch (IllegalArgumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
