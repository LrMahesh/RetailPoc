package com.retail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class RetailDriver {

	public static void main(String[] args) throws Exception {		
		System.out.println("Driver started");

		Configuration conf = new Configuration();
		Job job = new Job(conf, "word count");
		job.setJarByClass(RetailDriver.class);
		job.setJobName("Excel Record Reader");

		job.setNumReduceTasks(6);
		job.setMapperClass(RetailMapper.class);
		job.setPartitionerClass(CrawlingPartitioner.class);		
		job.setReducerClass(RetailReducer.class);;

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setInputFormatClass(ExcelInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		//imp steps

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}


}
