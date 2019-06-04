package com.bxl.mapreduce.weather;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by root on 2018/2/14.
 */
public class HotReduce extends Reducer<HotWeather, IntWritable, Text, IntWritable> {

    private Text kout = new Text();
    private IntWritable vout = new IntWritable();

    @Override
    protected void reduce(HotWeather key, Iterable<IntWritable> vals,Context context)
            throws IOException, InterruptedException {
        //首先，进到这里的数据经过分组比较器，年月都是相等的，需要做的就是比较不同日的温度，并记录温度最高的两天
        //先拿到两天的温度，然后用后边的温度和其相比，如果有高于其中的就替换掉
        /**
         * 进入这里的数据格式为：（首先是年月一致，其次由于自定义了比较器，所以日期按正序排列，温度按倒叙排列,
         * 						所以只需要取出每天的第一条即可，因为第一条就是某天的最高温度）
         *			1949-10-01 19:21:02	38c
         *			1949-10-01 14:21:02	34c
         *			1949-10-01 12:23:02 28c
         *			1949-10-02 14:01:02	36c
         */

        int wd1 = 0;
        int wd2 = 0;
        int d1 = 0;
        int d2 = 0;

        int day = 0;
        int flg = 0;

        for (IntWritable val : vals) {
            //首次
            if (flg == 0){
                day = key.getDay();
                d1 = day;
                wd1 =  key.getHot();
                flg++;
            }
            //第二次
            if(day != key.getDay()) {
                //是不是第三次之后
                if(flg==1){
                    d2=key.getDay();
                    wd2 = key.getHot();
                    flg++;
                }else{
                    //先判断，温度有没有比缓存大的
                    if ( wd1 > wd2   ){
                        if (key.getHot()> wd2){
                            wd2=key.getHot();
                            d2=key.getDay();
                        }
                    }else{
                        if (key.getHot()> wd1){
                            wd1=key.getHot();
                            d1=key.getDay();
                        }
                    }
                }
            }
        }

        kout.set(key.getYear()+"-"+key.getMouth()+"-"+d1);
        vout.set(wd1);
        context.write(kout, vout);

        if (flg == 2){

            kout.set(key.getYear()+"-"+key.getMouth()+"-"+d2);
            vout.set(wd2);
            context.write(kout, vout);
        }


    }
}
