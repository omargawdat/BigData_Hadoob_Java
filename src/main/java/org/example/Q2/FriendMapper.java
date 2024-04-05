package org.example.Q2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text user = new Text();

    private final Text friend = new Text();

    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split(",");
        if (parts.length != 2)
            return;
        this.user.set(parts[0].trim());
        this.friend.set(parts[1].trim());
        context.write(this.user, this.friend);
        context.write(this.friend, this.user);
    }
}