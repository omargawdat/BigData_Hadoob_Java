package org.example.Q2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FriendDriver extends Configured implements Tool {
    public int run(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job firstJob = new Job(conf, "Friends");
        firstJob.setJarByClass(FriendDriver.class);
        firstJob.setMapperClass(FriendMapper.class);
        firstJob.setReducerClass(FriendReducer.class);
        firstJob.setOutputKeyClass(Text.class);
        firstJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(firstJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(firstJob, new Path(args[1]));
        firstJob.waitForCompletion(true);
        Job secondJob = new Job(conf, "FriendOfFriends");
        secondJob.setJarByClass(FriendDriver.class);
        secondJob.setMapperClass(MultiFriendMapper.class);
        secondJob.setReducerClass(MultiFriendReducer.class);
        secondJob.setOutputKeyClass(Text.class);
        secondJob.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(secondJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(secondJob, new Path(args[2]));
        System.exit(secondJob.waitForCompletion(true) ? 0 : 1);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new FriendDriver(), args);
        System.out.println(exitCode);
    }
}