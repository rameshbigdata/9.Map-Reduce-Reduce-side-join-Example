package com.ramesh.ReduceJoin;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class SpeedHlogDeltaDataMapper extends Mapper<Object, Text, Text, Text> {
private Text outkey = new Text();
private Text outvalue = new Text();
public static final String COMMA = ",";
@Override
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
String[] values = value.toString().split(",", -1);
String neid = values[5];
String portid = values[6];
outkey.set(neid + portid);
outvalue.set("H" + values[4] + COMMA + values[5] + COMMA + values[6] + COMMA
+ values[7]+COMMA+values[8]);
context.write(outkey, outvalue);
}
}