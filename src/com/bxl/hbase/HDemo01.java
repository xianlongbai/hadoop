package com.bxl.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;


import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

/**
 * @author root
 *
创建namespace
hbase>create_namespace 'ai_ns'
删除namespace
hbase>drop_namespace 'ai_ns'
查看namespace
hbase>describe_namespace 'ai_ns'
列出所有namespace
hbase>list_namespace
在namespace下创建表
hbase>create 'ai_ns:testtable', 'fm1'
查看namespace下的表
hbase>list_namespace_tables 'ai_ns'
 */

public class HDemo01 {

	private static Configuration conf= null; //
	private static HBaseAdmin admin = null;
	private static HTable table = null;

	@Before
	public void begin(){
		//方法1，必须将hbase-site.xml放在classpath目录下
		conf = HBaseConfiguration.create();
		//方法2
		//conf = new Configuration();
		// 指定hbase的zk集群
		// 如果是伪分布式 指定伪分布式那台服务器
		//conf.set("hbase.zookeeper.quorum", "node01,node02,node03");
	}
	/**
	 * HBase系统默认定义了两个缺省的namespace
	 hbase：系统内建表，包括namespace和meta表
	 default：用户建表时未指定namespace的表都创建在此
	 * @throws IOException
	 * @throws ZooKeeperConnectionException
	 * @throws MasterNotRunningException
	 */
	@Test  //创建表
	public void creatDB() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		admin =  new  HBaseAdmin(conf);
		//创建名为bxl的namespace
		admin.createNamespace(NamespaceDescriptor.create("test01").build());
		//指定要创建的表名
		HTableDescriptor htb = new HTableDescriptor(TableName.valueOf("bxl:td_01"));
		//指定表的列族名称
		HColumnDescriptor hcol = new HColumnDescriptor("cf".getBytes());
		//开启缓存
		hcol.setInMemory(true);
		//设置最大版本数
		hcol.setMaxVersions(2);
		//日志flush的时候是同步写，还是异步写
		htb.setDurability(Durability.SYNC_WAL );
		//添加列族
		htb.addFamily(hcol);
		//创建表
		admin.createTable(htb);
		admin.close();

	}

	@Test //删除表
	public void deleteDb() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		admin =  new  HBaseAdmin(conf);
		if(admin.tableExists("test02".getBytes())){
			admin.disableTable("test02".getBytes());
			admin.deleteTable("test02".getBytes());
		}
		admin.close();

	}

	@Test //修改表的列族--删除一个列族cf2，增加一个列族cf3
	public void alertColmFamily() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		admin =  new  HBaseAdmin(conf);
		if(admin.tableExists("test01".getBytes())){
			//先将表禁用再修改
			admin.disableTable("test01".getBytes());
			HTableDescriptor htb = admin.getTableDescriptor(Bytes.toBytes("test01"));
			//删除cf2列族
			htb.removeFamily("cf2".getBytes());
			//创建一个新的列族
			HColumnDescriptor hcol = new HColumnDescriptor("cf3".getBytes());
			hcol.setMaxVersions(2);
			hcol.setKeepDeletedCells( true );  //被删除的数据在基于时间的历史数据查询中依然可见
			htb.addFamily(hcol);
			//修改表结构
			admin.modifyTable(Bytes.toBytes("test01"), htb);
			admin.enableTable("test01".getBytes());
			admin.close();
		}
	}

	@Test  //修改现有列族属性
	public void alertColum() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		admin =  new  HBaseAdmin(conf);
		if(admin.tableExists("test01".getBytes())){
			admin.disableTable("test01".getBytes());
			HTableDescriptor htb = admin.getTableDescriptor(Bytes.toBytes("test01"));
			HColumnDescriptor hcol = htb.getFamily(Bytes.toBytes("cf1"));
			hcol.setMaxVersions(3);
			admin.modifyColumn("test01".getBytes(), hcol);
			admin.enableTable("test01".getBytes());
			admin.close();
		}
	}

	@Test//向表中添加数据
	public void insertRow() throws MasterNotRunningException, ZooKeeperConnectionException, IOException{
		table = new HTable(conf, "test01".getBytes());
		Put put = new Put("rk002".getBytes());
		put.add("cf1".getBytes(), "sex".getBytes(), "男".getBytes());
		put.add("cf1".getBytes(), "age".getBytes(), "15".getBytes());
		put.setDurability(Durability. SYNC_WAL );
		table.put(put);
		table.close();

	}
	/**
	 * @throws IOException
	 * Put的构造函数都需要指定行键，如果是全新的行键，则新增一行；如果是已有的行键，则更新现有行。
	 *
	 * Put put =  new  Put(Bytes. toBytes ( "100001_100002" ),7,6)
	 * 第二个参数是偏移量，也就是行键从第一个参数的第几个字符开始截取；第三个参数是截取长度；
	 */
	@Test//更新表中添加数据
	public void updateRow() throws IOException{
		table = new HTable(conf, "test01".getBytes());
		Put put = new Put("rk002".getBytes());
		put.add("cf1".getBytes(), "sex".getBytes(), "女".getBytes());
		put.add("cf1".getBytes(), "age".getBytes(), "18".getBytes());
		put.setDurability(Durability. SYNC_WAL );
		table.put(put);
		table.close();
	}

	@Test //删除整行的所有列族、所有行、所有版本
	public void deleteByRowToAll() throws IOException{
		table = new HTable(conf, "test01".getBytes());
		Delete del = new Delete("rk02".getBytes());
		table.delete(del);
		table.close();
	}

	@Test
	public void deleteByRowToOne() throws IOException{
		table = new HTable(conf, "test01".getBytes());
		Delete del = new Delete("rk02".getBytes());
		//del.deleteColumn("cf1".getBytes(), "age".getBytes()); //删除 指定列的最新版本
		//del.deleteColumns("cf1".getBytes(), "age".getBytes());  //删除 指定列的所有版本
		del. deleteFamilyVersion (Bytes. toBytes ( "cf1" ), 1405390959464L);//删除cf1列族中，时间戳为1405390959464的所有列数据
		table.delete(del);
		table.close();
	}

	/**
	 * 中文乱码
	 * @throws IOException
	 */
	@Test //获取行键指定行的 所有列族、所有列 的 最新版本 数据
	public void getRow() throws IOException{
		table = new HTable(conf, "test01".getBytes());
		Get get =  new  Get(Bytes. toBytes ( "rk002" ));
		//获取行键指定行中， 指定列 的最新版本数据
		//get.addColumn(Bytes. toBytes ( "cf1" ), Bytes. toBytes ( "sex" ));
		//获取行键指定的行中， 指定时间戳 的数据
		// get.setTimeStamp(1405407854374L);
		//获取行键指定的行中， 所有版本 的数据
		// get.setMaxVersions();
		Result rs = table.get(get);
		for  (Cell cell : rs.rawCells()) {
			System. out .println(
					"Rowkey : " +Bytes.toString(rs.getRow())+
							"   Familiy:Quilifier : " +Bytes.toString (CellUtil.cloneQualifier(cell))+
							"   Value : " +Bytes.toString(CellUtil. cloneValue(cell))+"----"+new String(CellUtil. cloneValue(cell))
			);
		}
		table.close();
	}

	@Test //扫描表中的 所有行 的最新版本数据
	public void ScanRows() throws IOException{
		table = new HTable(conf, "test01".getBytes());
		Scan s =  new  Scan();
		//扫描指定行键范围，通过末尾加0，使得结果集包含StopRow
		//s. setStartRow (Bytes. toBytes ( "rk001" ));
		//s. setStopRow (Bytes. toBytes ( "rk002" ));
		//s. setStopRow (Bytes. toBytes ( " rk0020" ));
		// 获得所有 已经被打上删除标记但尚未被真正删除 的数据
		//s.setRaw( true );
		//s.setMaxVersions();

		ResultScanner rs = table.getScanner(s);
		for  (Result r : rs) {
			//可以直接拿到最新版本cell(单元)的一个对象
			//r..getColumnLatestCell("cf1".getBytes(), "name".getBytes());
			for  (Cell cell : r.rawCells()) {
				System. out .println(
						"Rowkey : " +Bytes. toString (r.getRow())+
								"   Familiy:Quilifier : " +Bytes. toString (CellUtil. cloneQualifier(cell))+
								"   Value : " +Bytes. toString (CellUtil. cloneValue (cell))+
								"   Time : " +cell.getTimestamp()
				);
			}
		}
		table.close();
	}

	/**
	 * @throws IOException
	FilterList分为二种类型，如下
	FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ONE);
	或者
	FilterList allFilters = new FilterList(FilterList.Operator.MUST_PASS_ALL);
	二种区别根据上述注释理解，其实就相当于and和or的关系.MUST_PASS_ONE只要scan的数据行符合其中一个filter就可以返回结果(但是必须扫描所有的filter)，
	另外一种MUST_PASS_ALL必须所有的filter匹配通过才能返回数据行(但是只要有一个filter匹配没通过就算失败，后续的filter停止匹配)

	 */

	@Test  // 结合过滤器，获取所有age在20到30之间的行
	public void findbyFilter() throws IOException{
		table = new HTable(conf, "test01".getBytes());
		FilterList filterList =  new  FilterList(FilterList.Operator. MUST_PASS_ALL );
		SingleColumnValueFilter filter1 =  new  SingleColumnValueFilter(
				Bytes. toBytes ( "cf1" ),
				Bytes. toBytes ( "age" ),
				CompareOp. GREATER_OR_EQUAL ,
				Bytes. toBytes ( "10" )
		);
		SingleColumnValueFilter filter2 =  new  SingleColumnValueFilter(
				Bytes. toBytes ( "cf1" ),
				Bytes. toBytes ( "age" ),
				CompareOp. LESS_OR_EQUAL ,
				Bytes. toBytes ( "30" )
		);
		filterList.addFilter(filter1);
		filterList.addFilter(filter2);
		Scan scan =  new  Scan();
		scan.setFilter(filterList);
		ResultScanner rs = table.getScanner(scan);
		for  (Result r : rs) {
			for  (Cell cell : r.rawCells()) {
				System. out .println(
						"Rowkey : " +Bytes. toString (r.getRow())+
								"   Familiy:Quilifier : " +Bytes. toString (CellUtil. cloneQualifier(cell))+
								"   Value : " +Bytes. toString (CellUtil. cloneValue (cell))+
								"   Time : " +cell.getTimestamp()
				);
			}
		}
		table.close();
	}
}
