package org.example.Q1_3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DifferenceReducer extends Reducer<Text, Text, Text, Text> {
    List<Integer> t1Values = new ArrayList<>();

    List<Integer> t2Values = new ArrayList<>();

    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String firstKey = "T1";
        for (Text value : values) {
            if (key.toString().equals("T1")) {
                this.t1Values.add(Integer.valueOf(Integer.parseInt(value.toString())));
                Collections.sort(this.t1Values);
                continue;
            }
            if (key.toString().equals("T2")) {
                this.t2Values.add(Integer.valueOf(Integer.parseInt(value.toString())));
                Collections.sort(this.t2Values);
            }
        }
        context.write(key, key.toString().equals("T1") ? new Text(this.t1Values.toString()) : new Text(this.t2Values.toString()));
        if (key.toString().equals("T2")) {
            List<Integer> diff = new ArrayList<>(this.t1Values);
            diff.removeAll(this.t2Values);
            context.write(new Text(String.valueOf(firstKey) + "-" + key), new Text(diff.toString()));
            List<Integer> secondDiff = new ArrayList<>(this.t2Values);
            secondDiff.removeAll(this.t1Values);
            context.write(new Text(key + "-" + firstKey), new Text(secondDiff.toString()));
        }
    }
}