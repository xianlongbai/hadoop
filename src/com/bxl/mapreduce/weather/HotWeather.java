package com.bxl.mapreduce.weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by root on 2018/2/14.
 */
public class HotWeather implements WritableComparable<HotWeather> {

    private int year=0;
    private int mouth=0;
    private int day=0;
    private int hot=0;

    public HotWeather() {
    }

    public HotWeather(int year, int mouth, int day, int hot) {
        this.year = year;
        this.mouth = mouth;
        this.day = day;
        this.hot = hot;
    }

    @Override
    public int compareTo(HotWeather that) {
        int c1 = Integer.compare(this.year, that.getYear());
        if(c1==0){
            int c2 = Integer.compare(this.mouth, that.getMouth());
            if(c2==0){
                int c3 = Integer.compare(this.day, that.getDay());
                if(c3==0){
                    return Integer.compare(this.hot, that.getHot());
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

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(mouth);
        out.writeInt(day);
        out.writeInt(hot);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.mouth = in.readInt();
        this.day = in.readInt();
        this.hot = in.readInt();
    }





    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMouth() {
        return mouth;
    }

    public void setMouth(int mouth) {
        this.mouth = mouth;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getHot() {
        return hot;
    }

    public void setHot(int hot) {
        this.hot = hot;
    }


}
