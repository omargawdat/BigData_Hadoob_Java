- Q1:
```
hadoop jar target/bigdata_3-1.0-SNAPSHOT.jar org.example.Q1_1.InnerDriver /input/Q1.txt /output/output1_1
hadoop jar target/bigdata_3-1.0-SNAPSHOT.jar org.example.Q1_2.FullOuterDriver1 /input/Q1.txt /output/output1_2
hadoop jar target/bigdata_3-1.0-SNAPSHOT.jar org.example.Q1_3.Difference /input/Q1.txt /output/output1_3
```
- Q2:
```
hadoop jar target/bigdata_3-1.0-SNAPSHOT.jar org.example.Q2.FriendDriver /input/Q2.txt /output/output2
```
- Q3:
```
hadoop jar target/bigdata_3-1.0-SNAPSHOT.jar org.example.Q3.KeyValueSort /input/Q3.txt /output/output3
```