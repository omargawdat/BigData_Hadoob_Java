package org.example.Q1_3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DifferenceReducer extends Reducer<Text, Text, Text, Text> {
    private List<Integer> t1Values = new ArrayList<>();
    private List<Integer> t2Values = new ArrayList<>();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if ("T1".equals(key.toString())) {
            for (Text value : values) {
                t1Values.add(Integer.parseInt(value.toString()));
            }
        } else if ("T2".equals(key.toString())) {
            for (Text value : values) {
                t2Values.add(Integer.parseInt(value.toString()));
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        t1Values.removeAll(t2Values);
        context.write(new Text("T1-T2"), new Text(t1Values.toString()));
    }
}