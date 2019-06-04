package com.bxl.mapreduce.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by root on 2018/2/14.
 */
public class HotMapper extends Mapper<Object, Text, HotWeather, IntWritable> {

    HotWeather hw = new HotWeather();
    IntWritable iw = new IntWritable();
    Text text = new Text();

    @Override
    protected void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            //1.读懂数据
            String[] strs = value.toString().split("\t");
            //2.将数据映射为K-V模型,也就是填充自定对象的属性值
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date = sdf.parse(strs[0]);
            Calendar cal = Calendar.getInstance(); cal.setTime(date);
            hw.setYear(cal.get(Calendar.YEAR));
            hw.setMouth(cal.get(Calendar.MONTH)+1);
            hw.setDay(cal.get(Calendar.DAY_OF_MONTH));
            int hot = Integer.parseInt(strs[1].substring(0, strs[1].length()-1));
            hw.setHot(hot);
            iw.set(hot);
            //3.输出数据
            context.write(hw, iw);//以对象的形式输出
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
