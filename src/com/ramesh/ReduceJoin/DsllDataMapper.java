package com.ramesh.ReduceJoin;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class DsllDataMapper extends Mapper<Object, Text, Text, Text> {
private Text outkey = new Text();
private Text outvalue = new Text();
public static final String COMMA = ",";
@Override
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
String data = value.toString();
String[] field = data.split(",", -1);
if (null != field && field.length > 62) {
String neid = field[2];
String portid = field[3];
outkey.set(neid + portid);
outvalue.set("D" + field[0] + COMMA + field[5] + COMMA + field[7].toString() + COMMA + field[62] + COMMA
+ field[63]);
context.write(outkey, outvalue);
}
}
}
