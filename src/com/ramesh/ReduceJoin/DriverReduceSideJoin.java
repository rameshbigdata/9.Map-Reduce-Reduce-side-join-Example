package com.ramesh.ReduceJoin;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class DriverReduceSideJoin {
public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
/*
* I have used my local path in windows change the path as per your
* local machine
*/
	args = new String[] { 
			"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/7.JoinTypes/ReduceSideJoin/Input_Path/sample_hlog.csv",
			"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/7.JoinTypes/ReduceSideJoin/Input_Path/sample_dsl.csv",
			"/media/hadoop/156d5b2f-6481-4a81-b1bb-56e2179981bb1/ramesh/2018 life/hadoop/DataFlair/Assignments/7.JoinTypes/ReduceSideJoin/Output_Path/"};
			 
			/* delete the output directory before running the job */
			FileUtils.deleteDirectory(new File(args[2])); 
			 
			if (args.length != 3) {
			System.err.println("Please specify the input and output path");
			System.exit(-1);
			}
			
			System.setProperty("hadoop.home.dir","/home/hadoop/work/hadoop-3.1.2");
			
	        Configuration conf =new Configuration();
conf.set("join.type","inner");
Job sampleJob = Job.getInstance(conf);
sampleJob.setJarByClass(DriverReduceSideJoin.class);
sampleJob.getConfiguration().set("mapreduce.output.textoutputformat.separator", ",");
TextOutputFormat.setOutputPath(sampleJob, new Path(args[2]));
sampleJob.setOutputKeyClass(Text.class);
sampleJob.setOutputValueClass(Text.class);
sampleJob.setReducerClass(SpeedHlogDslJoinReducer.class);
MultipleInputs.addInputPath(sampleJob, new Path(args[0]), TextInputFormat.class,
SpeedHlogDeltaDataMapper.class);
MultipleInputs.addInputPath(sampleJob, new Path(args[1]), TextInputFormat.class, DsllDataMapper.class);
@SuppressWarnings("unused")
int code = sampleJob.waitForCompletion(true) ? 0 : 1;
}
}
