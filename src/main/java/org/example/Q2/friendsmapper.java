package org.example.Q2;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
public class friendsmapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] part = value.toString().split(",");
        String personA = part[0];
        String personB = part[1];

        context.write(new Text(personA), new Text(personB));
        context.write(new Text(personB), new Text(personA));
    }
}