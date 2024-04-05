package org.example.Q2;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MultiFriendReducer extends Reducer<Text, Text, Text, Text> {
    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        Set<String> userfriendsfriends = new HashSet<>();
        for (Text value : values)
            userfriendsfriends.add(value.toString());
        String multifriends = userfriendsfriends.toString().replaceAll("\\[|\\]|\\s+", "");
        context.write(new Text(key), new Text(multifriends));
    }
}