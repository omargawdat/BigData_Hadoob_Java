package org.example.Q1_3;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DifferenceMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outputKey = new Text();

    private Text outputValue = new Text();

    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String tKey = fields[0];
        Integer attributeValue = Integer.valueOf(0);
        if (tKey.equals("T1")) {
            attributeValue = Integer.valueOf(Integer.parseInt(fields[1].trim())); // Added trim() here
            this.outputKey.set(tKey);
            this.outputValue.set(attributeValue.toString());
            context.write(this.outputKey, this.outputValue);
        } else if (tKey.equals("T2")) {
            attributeValue = Integer.valueOf(Integer.parseInt(fields[2].trim())); // And here
            this.outputKey.set(tKey);
            this.outputValue.set(attributeValue.toString());
            context.write(this.outputKey, this.outputValue);
        }
    }
}