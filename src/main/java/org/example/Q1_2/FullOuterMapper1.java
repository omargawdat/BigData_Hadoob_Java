package org.example.Q1_2;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FullOuterMapper1 extends Mapper<Object, Text, Text, Text> {
    private Text value = new Text();

    private Text primaryKey = new Text();

    public void map(Object key, Text record, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        String[] fields = record.toString().split(",");
        String tableName = fields[0];
        String pk = fields[1];
        String attributeValue = fields[2];
        if (tableName.equals("T1")) {
            this.primaryKey.set(pk);
            this.value.set("T1," + attributeValue);
            System.out.print(this.value);
            context.write(this.primaryKey, this.value);
        } else if (tableName.equals("T2")) {
            this.primaryKey.set(attributeValue);
            this.value.set("T2," + pk);
            context.write(this.primaryKey, this.value);
        }
    }
}