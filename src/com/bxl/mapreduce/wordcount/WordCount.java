package com.bxl.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * Created by root on 2018/2/1.
 */
public class WordCount {

    private static void wordCount(String input,String output) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration(true);
        conf.set("mapreduce.app-submission.cross-platform", "true");
//        conf.set("mapreduce.framework.name", "local");
        Job job = Job.getInstance(conf);
        job.setJarByClass(WordCount.class);
        job.setJar("D:\\tmp\\intest\\test_wc.jar");
        job.setJobName("wordcount");
        //指定文件的读取方式
        //job.setInputFormatClass(MyInputFormat.class);
        Path inputpath = new Path(input);
        FileInputFormat.addInputPath(job, inputpath);
        Path outputPath = new Path(output);
        if(outputPath.getFileSystem(conf).exists(outputPath)){
            outputPath.getFileSystem(conf).delete(outputPath);
        }
        FileOutputFormat.setOutputPath(job, outputPath);
        job.setMapperClass(MyMapper.class);
//下面这种写法，指定的是map和reduce两个阶段的输出的k,v
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

        // Submit the job, then poll for progress until the job is complete
        if (job.waitForCompletion(true)){
            System.out.println("任务执行完成!!!");
        }

    }

    /**
     * 重写文件的input方法
     */
    private  static class MyInputFormat extends FileInputFormat<Text, Text>{

        @Override
        public RecordReader<Text, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
                throws IOException, InterruptedException {
            return new MyRecordReader();
        }
    }


    private  static class MyMapper extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()){
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    private static class MyReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        private  IntWritable result = new IntWritable();
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        wordCount("/user/root/friend","/user/root/result");
//        wordCount("D:\\tmp\\intest\\aaa.txt","D:\\tmp\\intest\\wc");
//    System.out.println("任务执行完成!!!");
    }
}
