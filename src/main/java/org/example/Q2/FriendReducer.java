package org.example.Q2;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FriendReducer extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();
    private Text emptyText = new Text(" ");

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> friendsList = new ArrayList<>();
        for (Text val : values) {
            friendsList.add(val.toString());
        }
        if (friendsList.contains("*")) {
            for (String friend : friendsList) {
                if (!friend.equals("*")) {
                    result.set(friend);
                    context.write(result, emptyText);
                }
            }
        }
    }
}
