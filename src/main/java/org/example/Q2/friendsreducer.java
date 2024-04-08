package org.example.Q2;

import java.io.IOException;
import java.util.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
public class friendsreducer extends Reducer<Text, Text, Text, Text> {
    private Map<String, Set<String>> directFriendsMap = new HashMap<>();
    private Map<String, Set<String>> friendsOfFriendsMap = new HashMap<>();
    private final Text p1Key = new Text("P1");
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> directFriends = new HashSet<>();
        for (Text value : values) {
            directFriends.add(value.toString());
        }
        directFriendsMap.put(key.toString(), directFriends);
    }
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Set<String> directFriendsOfP1 = directFriendsMap.get(p1Key.toString());
        if (directFriendsOfP1 != null) {
            Set<String> friendsOfFriendsOfP1 = new HashSet<>();
            for (String friendOfP1 : directFriendsOfP1) {
                Set<String> friendsOfFriend = directFriendsMap.get(friendOfP1);
                if (friendsOfFriend != null) {
                    friendsOfFriendsOfP1.addAll(friendsOfFriend);
                }
            }

            friendsOfFriendsOfP1.remove("P1");
            friendsOfFriendsOfP1.removeAll(directFriendsOfP1);
            context.write(p1Key, new Text(friendsOfFriendsOfP1.toString()));
        }
    }
}