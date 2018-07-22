package com.retail;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RetailMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws InterruptedException, IOException {
				
		String [] line = value.toString().split("\t");
		String rtlID = line[0];
		String rtlName = line[1];
		String typeOfCrawling = line[2];	
		String prodURL = line[3];	
		String title = line[4];	
		String salePrice = line[5];	
		String regPrice = line[6];	
		String rebatePercent = line[7];	
		String stockInfo = line[8];

		float salePriceFloat = Float.parseFloat(salePrice);
		float regPriceFloat = Float.parseFloat(regPrice);
		float rebatePercentInt = Float.parseFloat(rebatePercent);

		String productType = "OtherProducts";

		if (typeOfCrawling.equalsIgnoreCase("BS"))
		{
			if (salePriceFloat < 100.00  && rebatePercentInt > 50)				
				productType = "HighBuzzProducts";
			if (regPriceFloat < 150.00 && rebatePercentInt > 25 && rebatePercentInt < 50)				
				productType = "NormalProducts";
			if (title.length() > 100) 				
				productType = "RareProducts";
		}
		else if (typeOfCrawling.equalsIgnoreCase("ODC"))
		{
			if (salePriceFloat < 150.00)				
				productType = "OnDemandCrawlProducts";
			if (stockInfo.equalsIgnoreCase("InStock")) 				
				productType = "AvailableProducts";
		}
		context.write(new Text(rtlID+"\t"+rtlName+"\t"+typeOfCrawling+"\t"+prodURL+"\t"+title+"\t"+salePrice+"\t"+regPrice+"\t"+rebatePercent+"\t"+stockInfo), new Text(productType));
	}

}


