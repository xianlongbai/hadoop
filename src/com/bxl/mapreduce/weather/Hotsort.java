package com.bxl.mapreduce.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by root on 2018/2/14.
 */
public class Hotsort extends WritableComparator {

    //自定义比较器的实现必须调用父类的构造器,切记有两个参数，第二个参数不写，默认为false
    public Hotsort() {
        super(HotWeather.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        HotWeather hwthis = (HotWeather)a;
        HotWeather hwthat = (HotWeather)b;
        int c1 = Integer.compare(hwthis.getYear(), hwthat.getYear());
        if(c1==0){
            int c2 = Integer.compare(hwthis.getMouth(), hwthat.getMouth());
            if(c2==0){
                int c3 = Integer.compare(hwthis.getDay(), hwthat.getDay());
                if(c3==0){
                    return -Integer.compare(hwthis.getHot(), hwthat.getHot());
                }else{
                    return c3;
                }
            }else{
                return c2;
            }
        }else{
            return c1;
        }
    }
}
