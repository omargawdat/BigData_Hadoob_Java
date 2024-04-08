package org.example.Q1_3;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DifferenceMapper extends Mapper<LongWritable, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");
        String tKey = fields[0];
        if ("T1".equals(tKey) || "T2".equals(tKey)) {
            int fieldIndex = "T1".equals(tKey) ? 1 : 2;
            Integer attributeValue = Integer.parseInt(fields[fieldIndex].trim());
            outputKey.set(tKey);
            outputValue.set(attributeValue.toString());
            context.write(outputKey, outputValue);
        }
    }
}