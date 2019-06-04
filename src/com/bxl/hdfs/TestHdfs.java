package com.bxl.hdfs;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceStatus;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HAUtil;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.tools.DFSHAAdmin;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHdfs extends DFSHAAdmin {
    Configuration conf = null;
    FileSystem fs = null;
    FileSystem fs2 = null;

    @Before
    public void conn() throws IOException, InterruptedException, URISyntaxException{
        conf = new Configuration(true);
//		conf = new Configuration(false);
//		conf.set("fs.defaultFS", "hdfs://node1:8020");
        conf.set("dfs.blocksize", "5242880");  //设置block块大小为5m

//        conf.setBoolean("mapreduce.app-submission.cross-platform", true);// 配置使用跨平台提交任务
//        conf.set("fs.defaultFS", "hdfs://" + namenode + ":8020");// 指定namenode
//        conf.set("mapreduce.framework.name", "yarn"); // 指定使用yarn框架
//        conf.set("yarn.resourcemanager.address", resourcenode + ":8032"); // 指定resourcemanager
//        conf.set("yarn.resourcemanager.scheduler.address", schedulernode + ":8030");// 指定资源分配器
//        conf.set("mapreduce.jobhistory.address", jobhistorynode + ":10020");// 指定historyserver

//		拿到一个文件系统操作的客户端实例对象
        fs = FileSystem.get(conf);
//		LocalFileSystem local = FileSystem.getLocal(conf);
//		fs = FileSystem.get(new URI("hdfs://node1:8020"), conf, "root"); //最后一个参数为用户名
//		fs2 = FileSystem.getLocal(conf);
    }

    @After
    public void close() throws IOException{
        fs.close();
    }

    @Test
    public void testConn(){
        System.out.println(conf.get("fs.defaultFS"));
        System.out.println(conf.get("dfs.blocksize"));
    }

    /**
     * 查看文件信息
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testLook() throws IllegalArgumentException, IOException{
        FileStatus stat = fs.getFileStatus(new Path("/user/root/bxl/wc.txt"));
        System.out.print(stat.getAccessTime()+" "+stat.getBlockSize()+" "+stat.getGroup()
                +" "+stat.getLen()+" "+stat.getModificationTime()+" "+stat.getOwner()
                +" "+stat.getReplication()+" "+stat.getPermission()
        );
    }

    /**
     * 查看目录文件
     * @throws IOException
     */
    @Test
    public void testFindDir () throws IOException{
        Path path = new Path("/user/root/bxl");
        FileStatus status[] = fs.listStatus(path);
        for (int i = 0; i < status.length; i++) {
            System.out.println(status[i].getPath().toString());
        }

    }

    /**
     * 文件重命名
     * @throws IOException
     */
    @Test
    public void testRenameFile () throws IOException{
        Path oldpath = new Path("/user/root/bxl/wc.txt");
        Path newpath = new Path("/user/root/bxl/newwc.txt");
        boolean isok = fs.rename(oldpath, newpath);
        System.out.println("文件重命名："+isok);
    }

    /**
     * 查找某个文件在 HDFS集群的位置,以及偏移量的大小
     *
     * @param
     * @return BlockLocation[]
     */
    @Test
    public void testFileBlockLocations() throws IOException {
        // 文件路径
        String hdfsUri = "/user/root/bxl/";
        String filePath = "aa.txt";
        if(StringUtils.isNotBlank(hdfsUri)){
            filePath = hdfsUri + filePath;
        }
        Path path = new Path(filePath);
        // 文件块位置列表
        BlockLocation[] blkLocations = new BlockLocation[0];
        // 获取文件目录
        FileStatus filestatus = fs.getFileStatus(path);
        //获取文件块位置列表
        blkLocations = fs.getFileBlockLocations(filestatus, 0, filestatus.getLen());
        for (BlockLocation blkLocation : blkLocations) {
            System.out.println("块信息："+blkLocation);
        }

    }


    /**
     * 查看datanode 信息
     * @throws IOException
     */
    @Test
    public void testFindDataNode () throws IOException{
        DistributedFileSystem hdfs = (DistributedFileSystem) FileSystem.get(conf);
        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
        for (DatanodeInfo dataNode : dataNodeStats) {
            System.out.println(dataNode.getHostName() + "\t" + dataNode.getName() +"\t" +dataNode.getInfoAddr());
        }
    }

    /**
     * 查看Namenode 信息
     * @throws IOException
     */
    @Test
    public void testFindNameNode () throws IOException{

        String nsId = DFSUtil.getNamenodeNameServiceId(conf);
        Collection<String> nameNodeIds = DFSUtil.getNameNodeIds(conf, nsId);
//        String nnId = HAUtil.getNameNodeId(conf, nsId);
        System.out.println("服务ID:"+nsId+" ,nnids:"+nameNodeIds);
        System.out.println("------------------------------------");
        //有问题
//        String nnidStr = "nn1,nn2";
//        String[] nnids = nnidStr.split(",",-1);
//        int rpcTimeoutForChecks = -1;
//
//        for (String nnid : nnids) {
//            HAServiceTarget haServiceTarget = resolveTarget(nnid);
//            HAServiceProtocol proto = haServiceTarget.getProxy(conf, rpcTimeoutForChecks);
//            HAServiceStatus state = proto.getServiceStatus();
//            if (state.getState().ordinal()==HAServiceProtocol.HAServiceState.ACTIVE.ordinal()) {
//                System.out.println( haServiceTarget.getAddress().getHostName() + ":" + haServiceTarget.getAddress().getPort()+haServiceTarget.getAddress().getHostName()+":"+haServiceTarget.getAddress().getPort());
//            }
//        }

    }


    /**
     * 创建文件夹
     * @throws IOException
     */
    @Test
    public void testMkdir() throws IOException{
        Path path = new Path("/user/root/hyx");
        if(!fs.exists(path)){
            boolean flag = fs.mkdirs(path);
            System.out.println(flag);
        }
    }

    /**
     * 上传文件
     * @throws IOException
     */
    @Test
    public void testPufile() throws IOException{
        //上传的目标位置
        Path outPath = new Path("/user/root/bxl");

        Path path0 = new Path("D:\\tmp\\intest\\tfidf\\weibo_02");
        Path path1 = new Path("C:\\Users\\16050\\Desktop\\test\\aa.txt");
        Path path3 = new Path("C:\\Users\\16050\\Desktop\\test\\周报.txt");
        Path path4 = new Path("C:\\Users\\16050\\Desktop\\test\\jetty.tar.gz");
        //上传文件，并且删掉本地的文件
        fs.copyFromLocalFile(false, path0,outPath);

        //上传文件，不删掉本地的文件,且覆盖原有文件
        //fs.copyFromLocalFile(false, true, path1, outPath);

        //上传文件，不删掉本地的文件,不覆盖原有文件
        //fs.copyFromLocalFile(false, false, path3, outPath);
        System.out.println("上传成功！！！");
    }

    /**
     * 下载文件
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testDownFile() throws IllegalArgumentException, IOException{
        //在下载的时候会同时下载一个.crc文件，如果不需要可自行删除
        fs.copyToLocalFile(new Path("/user/root/bxl/aa.txt"), new Path("C:\\Users\\16050\\Desktop\\test"));
        System.out.println("下载成功！！！");
    }

    /**
     * 删除文件或者目录
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testDeleteFile() throws IllegalArgumentException, IOException{

        boolean flag = fs.delete(new Path("/user/root/bxf"), true);//等于true为递归删除，包括文件夹
        System.out.println(flag);
    }

    //模仿hadoop fs -put 和 -copyFromLoca命令，实现本地复制文件到hdfs
    @Test
    public void testIOtoHdfs () throws IOException{

        String local = "C:\\Users\\16050\\Desktop\\test\\jetty.tar.gz";
        String hdfs = "/user/root/bxl/jetty.tar.gz";
        InputStream in = new BufferedInputStream(new FileInputStream(local));
//			OutputStream out = fs.create(new Path(hdfs), new Progressable() {
//				public void progress() {
//					System.out.print(".");
//				}
//			});

        OutputStream out = fs.create(new Path(hdfs),false);
        IOUtils.copyBytes(in, out, 4096, true);
        System.out.println("上传成功！！！");
    }

    //模仿hadoop fs -get 和 -copyFromLoca命令，实现hdfs复制文件到本地
    @Test
    public void testIOtoLocal () throws IOException{

        String local = "C:\\Users\\16050\\Desktop\\mmmm.rar";
        String hdfs = "/user/root/bxl/maven1.rar";

        //InputStream in = new BufferedInputStream(new FileInputStream(hdfs));
        OutputStream out = new BufferedOutputStream(new FileOutputStream(local));
        //OutputStream out = fs.create(new Path(local),false);
        FSDataInputStream in = fs.open(new Path(hdfs));
        IOUtils.copyBytes(in, out, 4096, true);
    }


    /**
     * 给文件追加内容()
     * @throws IOException
     */
    @Test
    public void testAppendText () throws IOException {
        //conf.setBoolean("dfs.support.append", true);  //在集群中开启了，或者在在代码中加配置
        String hdfs_path = "/user/root/bxl/zhou.txt";//文件路径
        String inpath = "C:\\Users\\16050\\Desktop\\hadoop_pre\\aaa.txt";
        try {
            //要追加的文件流，inpath为文件
            InputStream in = new BufferedInputStream(new FileInputStream(inpath));
            OutputStream out = fs.append(new Path(hdfs_path));
            IOUtils.copyBytes(in, out, 4096, true);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 上传压缩文件
     * @throws IOException
     */
    @Test
    public void testUploadCompressFile() throws IOException, ClassNotFoundException {
        String codecClassName = "org.apache.hadoop.io.compress.BZip2Codec";
//		String codecClassName = "org.apache.hadoop.io.compress.GzipCodec";
        Class<?> codecClass = Class.forName(codecClassName);
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
        //指定压缩文件路径
//		String outFile = "/user/root/tq"+codec.getDefaultExtension(); //获得默认扩展名
        FSDataOutputStream outputStream = fs.create(new Path("/user/root/bxl/friend"+codec.getDefaultExtension()));
        //指定要被压缩的文件路径
        FSDataInputStream in = fs.open(new Path("/user/root/bxl/friend"));
//		FileInputStream in = new FileInputStream(new File("D:\\tmp\\tq.txt"));
        //创建压缩输出流
        CompressionOutputStream out = codec.createOutputStream(outputStream);
        IOUtils.copyBytes(in, out, conf);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }


    /**
     * 减压文件
     * @throws IOException
     */
    @Test
    public void testUnCompressFile() throws IOException, ClassNotFoundException {
        Class<?> codecClass = Class.forName("org.apache.hadoop.io.compress.GzipCodec");
        CompressionCodec codec = (CompressionCodec)ReflectionUtils.newInstance(codecClass, conf);
        FSDataInputStream inputStream = fs.open(new Path("/user/root/bxl/wc.gz"));
        //把text文件里到数据解压，然后输出到控制台
        InputStream in = codec.createInputStream(inputStream);
        FSDataOutputStream out = fs.create(new Path("/user/root/bxl/wc.txt"));
        //打印在控制台
        //IOUtils.copyBytes(in, System.out, conf);
        //输出到文件
        IOUtils.copyBytes(in, out, conf);
        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }


    /**
     * 使用文件扩展名推断而来的codec来对文件进行解压缩
     * @throws IOException
     * todo...
     */
    @Test
    public void testUnCompressFileWithShuff() throws IOException, ClassNotFoundException {
        String uri = "";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);

        Path inputPath = new Path(uri);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        CompressionCodec codec = factory.getCodec(inputPath);
        if(codec == null){
            System.out.println("no codec found for " + uri);
            System.exit(1);
        }
        String outputUri = CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
        InputStream in = null;
        OutputStream out = null;
        try {
            in = codec.createInputStream(fs.open(inputPath));
            out = fs.create(new Path(outputUri));
            IOUtils.copyBytes(in, out, conf);
        } finally{
            IOUtils.closeStream(out);
            IOUtils.closeStream(in);
        }
    }

    public static void main(String[] args){
        System.out.println(111);
    }

}
