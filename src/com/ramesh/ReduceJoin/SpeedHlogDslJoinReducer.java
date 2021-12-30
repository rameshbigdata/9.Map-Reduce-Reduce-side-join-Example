package com.ramesh.ReduceJoin;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class SpeedHlogDslJoinReducer extends Reducer<Text, Text, Text, Text> {
private ArrayList<Text> listH = new ArrayList<Text>();
private ArrayList<Text> listD = new ArrayList<Text>();
private static final Text EMPTY_TEXT = new Text("");
@Override
public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
listH.clear();
listD.clear();
for (Text text : values) {
if (text.charAt(0) == 'H') {
listH.add(new Text(text.toString().substring(1)));
} else if (text.charAt(0) == 'D') {
listD.add(new Text(text.toString().substring(1)));
}
}
executeJoinLogic(context);
}
private void executeJoinLogic(Context context) throws IOException, InterruptedException {
String joinType = context.getConfiguration().get("join.type");
if (joinType.equalsIgnoreCase("inner")) {
if (!listH.isEmpty() && !listD.isEmpty()) {
for (Text hlogData : listH) {
for (Text dslData : listD) {
context.write(hlogData, dslData);
}
}
}
} else if (joinType.equalsIgnoreCase("leftouter")) {
for (Text hlogData : listH) {
if (!listD.isEmpty()) {
for (Text dslData : listD) {
context.write(hlogData, dslData);
}
} else {
context.write(hlogData, EMPTY_TEXT);
}
}
} else if (joinType.equalsIgnoreCase("rightouter")) {
for (Text dslData : listD) {
if (!listH.isEmpty()) {
for (Text hlogData : listH) {
context.write(dslData, hlogData);
}
} else {
context.write(dslData, EMPTY_TEXT);
}
}
} else if (joinType.equalsIgnoreCase("fullouter")) {
if (!listH.isEmpty()) {
for (Text hlogData : listH) {
if (!listD.isEmpty()) {
for (Text dslData : listD) {
context.write(hlogData, dslData);
}
}
context.write(hlogData, EMPTY_TEXT);
}
} else {
for (Text dslData : listD) {
context.write(dslData, EMPTY_TEXT);
}
}
}
else if (joinType.equalsIgnoreCase("anti")) {
if (listH.isEmpty() ^ listD.isEmpty()) {
for (Text hlogData : listH) {
context.write(hlogData, EMPTY_TEXT);
}
for (Text dslData : listD) {
context.write(EMPTY_TEXT, dslData);
}
}
}
}
}
