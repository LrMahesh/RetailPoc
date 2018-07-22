package com.retail;


	

	import java.io.IOException;
	import org.apache.hadoop.io.IntWritable; 
	import org.apache.hadoop.io.Text;
	import org.apache.hadoop.mapreduce.Reducer;
	import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

	public class RetailReducer extends Reducer<Text, IntWritable, Text, IntWritable>
	{

	MultipleOutputs<Text, IntWritable>mos;

	@Override
	public void setup(Context context)
	//This method is called once to set the output key and value classes of MultipleOutput   //instance.
	{
		mos =new MultipleOutputs<Text, IntWritable>(context);
	}   

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException

	{
		// HADOOP , 1,1,1,1------> HADOOP	4
	   int sum=0;
	   for (IntWritable value : values) 
	    {
		   sum = sum + value.get();
	    }

	  if (sum>5)
	   {
		  mos.write("morethan5", key, new   IntWritable(sum),"/user/gopalnew/custom_output1/custom1");

		  // The above method will write the words with count more than 5 to a file at location         // "/user/edwaetlt/custom_output1/” in HDFS with name starting with ‘custom1’.
	   }
	  else if (sum<5)
	   {
		  mos.write("lessthan5", key, new  IntWritable(sum),"/user/gopalnew/custom_output2/custom2");
	   }
	  else
	   {
		  mos.write("equalto5", key, new IntWritable(sum),"/user/gopalnew/custom_output3/custom3");
		
	   }
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	  {
		mos.close();
	  }
	}




}
