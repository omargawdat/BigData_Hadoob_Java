package org.example.Q2;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FriendMapper extends Mapper<Object, Text, Text, Text> {
    private Text primaryFriend = new Text();
    private Text secondaryFriend = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer tokens = new StringTokenizer(value.toString(), ",");
        String initialFriend = "P1";
        while (tokens.hasMoreTokens()) {
            String currentToken = tokens.nextToken();
            if (currentToken.equals(initialFriend)) {
                currentToken = tokens.nextToken();
                primaryFriend.set(currentToken);
                secondaryFriend.set("*");
                context.write(primaryFriend, secondaryFriend);
            } else {
                primaryFriend.set(currentToken);
                currentToken = tokens.nextToken();
                secondaryFriend.set(currentToken);
                context.write(primaryFriend, secondaryFriend);
                context.write(secondaryFriend, primaryFriend);
            }
        }
    }
}
