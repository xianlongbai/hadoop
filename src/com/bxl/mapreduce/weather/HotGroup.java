package com.bxl.mapreduce.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by root on 2018/2/14.
 */
public class HotGroup extends WritableComparator {

    public HotGroup() {
        super(HotWeather.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        HotWeather hwthis = (HotWeather)a;
        HotWeather hwthat = (HotWeather)b;
        int c1 = Integer.compare(hwthis.getYear(), hwthat.getYear());
        if(c1==0){
            return Integer.compare(hwthis.getMouth(), hwthat.getMouth());
        }else{
            return c1;
        }
    }
}
