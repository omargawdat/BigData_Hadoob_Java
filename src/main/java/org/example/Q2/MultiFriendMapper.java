package org.example.Q2;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultiFriendMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text user = new Text();

    private Text multifriends = new Text();

    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\s+");
        String[] friends = parts[1].split(",");
        if (friends.length < 2)
            return;
        byte b;
        int i;
        String[] arrayOfString1;
        for (i = (arrayOfString1 = friends).length, b = 0; b < i; ) {
            String userfriend = arrayOfString1[b];
            this.user.set(userfriend);
            byte b1;
            int j;
            String[] arrayOfString;
            for (j = (arrayOfString = friends).length, b1 = 0; b1 < j; ) {
                String userfriends = arrayOfString[b1];
                if (!userfriend.equals(userfriends)) {
                    this.multifriends.set(userfriends);
                    context.write(this.user, this.multifriends);
                }
                b1++;
            }
            b++;
        }
    }
}