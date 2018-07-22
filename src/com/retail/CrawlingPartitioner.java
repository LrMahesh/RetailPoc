package com.retail;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CrawlingPartitioner extends Partitioner<Text, Text> {

	
	public int getPartition(Text key, Text value, int numReduceTasks) {
		
		if (value.toString().equalsIgnoreCase("HighBuzzProducts"))
		{				
			return 0;	
		}
		else if (value.toString().equalsIgnoreCase("NormalProducts"))
		{						
			return 1 % numReduceTasks;			
		}
		else if (value.toString().equalsIgnoreCase("RareProducts"))
		{						
			return 2 % numReduceTasks;			
		}
		else if (value.toString().equalsIgnoreCase("OnDemandCrawlProducts"))
		{						
			return 3 % numReduceTasks;			
		}
		else if (value.toString().equalsIgnoreCase("AvailableProducts"))
		{						
			return 4 % numReduceTasks;			
		}
		else
		{						
			return 5 % numReduceTasks;			
		}

	}
}
