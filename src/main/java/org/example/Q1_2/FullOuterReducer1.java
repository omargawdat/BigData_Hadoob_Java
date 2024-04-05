package org.example.Q1_2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FullOuterReducer1 extends Reducer<Text, Text, Text, Text> {
    private Text value = new Text();

    public void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String t1Value = null;
        String t2Value = null;
        for (Text val : values) {
            String[] fields = val.toString().split(",");
            String tableName = fields[0];
            String attributeValue = fields[1];
            if (tableName.equals("T1")) {
                t1Value = attributeValue;
                continue;
            }
            if (tableName.equals("T2"))
                t2Value = attributeValue;
        }
        if (t1Value == null) {
            this.value.set(String.format("%s,null,%s", new Object[] { key.toString(), "null" }));
        } else if (t2Value == null) {
            this.value.set(String.format("%s,%s,%s", new Object[] { t2Value, key.toString(), t1Value }));
        } else {
            this.value.set(String.format("%s,%s,%s", new Object[] { t2Value, key.toString(), t1Value }));
        }
        context.write(null, this.value);
    }
}