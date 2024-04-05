package org.example.Q1_1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InnerDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length < 2) {
            System.out.println("Please give valid inputs");
            System.exit(0);
        }
        Job job = Job.getInstance();
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(InnerMapper.class);
        job.setJarByClass(InnerDriver.class);
        job.setReducerClass(InnerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}