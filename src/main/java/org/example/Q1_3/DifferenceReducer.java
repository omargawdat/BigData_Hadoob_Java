package org.example.Q1_3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class DifferenceReducer extends Reducer<Text, Text, Text, Text> {
    private List<Integer> t1Values = new ArrayList<>();
    private List<Integer> t2Values = new ArrayList<>();
    private List<Integer> t1NotInT2 = new ArrayList<>();
    private List<Integer> t2NotInT1 = new ArrayList<>();

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
        t1NotInT2.addAll(t1Values);
        t1NotInT2.removeAll(t2Values);
        t2NotInT1.addAll(t2Values);
        t2NotInT1.removeAll(t1Values);
        context.write(new Text("T1-T2"), new Text(t1NotInT2.toString()));
        context.write(new Text("T2-T1"), new Text(t2NotInT1.toString()));
    }
}