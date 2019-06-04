package com.bxl.mapreduce.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by root on 2018/2/14.
 * /**
 * 这里应用最基本的以key的hash取模来分区（可以有多种实现）
    注意：既要保证相同的key去到一个parttion中，而且还要避免数据倾斜问题
 */

public class HotPartition extends Partitioner<HotWeather, IntWritable> {


    /**
     * 这里应用最基本的以key的hash取模来分区（可以有多种实现）
     * 但必须保证相同的年月去到同一个分区中
     */

//分到了同一个reduce中,虽然数据正确，但没有意义（失败）
//	@Override
//	public int getPartition(HotWeather key, IntWritable value, int numPartitions) {
//		return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
//	}

//以下分区算法不能保证相同的key为一组，所以导致计算数据混乱 （失败）
//	@Override
//	public int getPartition(HotWeather key, IntWritable value, int numPartitions) {
//		Object obj = key.getYear()+key.getMouth()+key.getDay()+key.getHot();
//		return obj.hashCode() % numPartitions;
//	}
//--------------------------------------------------以下都可以
//	@Override
//	public int getPartition(HotWeather key, IntWritable value, int numPartitions) {
//		return key.getYear() % numPartitions;
//	}

    @Override
    public int getPartition(HotWeather key, IntWritable value, int numPartitions) {
        Object obj = key.getYear()+key.getMouth();
        return obj.hashCode() % numPartitions;
    }
}
