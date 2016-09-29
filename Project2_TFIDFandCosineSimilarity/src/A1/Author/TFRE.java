package A1.Author;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TFRE extends Reducer<Text,Text,Text,Text> {
	
	//for counting no of authors
	public static enum TestCounters { TEST }
	public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
	{
		int max=0;
		int value=0;
		
		
		Map<String,Integer>hashset=new HashMap<String,Integer>();
		Map<String,Integer>Authorset=new HashMap<String,Integer>();
		
		for(Text val:values){
			
			String[] wordcount=val.toString().split("=");
			hashset.put(wordcount[0],Integer.valueOf(wordcount[1]));
			value=Integer.parseInt(val.toString().split("=")[1]);
			
			if(value>max)
			{
				max=value;
			}
			
			
			
			
		}
		
		if(!Authorset.containsKey(key))
		{
			Authorset.put(key.toString(), 1);
			
			//Using Hadoop Counters
			//context.getCounter(TestCounters.TEST).increment(1);
		}
		
		
		
		for (String wordasKey : hashset.keySet()) {
			
            context.write(new Text(key.toString()), new Text(wordasKey+"\t"+max+"\t"+(float)hashset.get(wordasKey)/max));
        }
		
		
	}



}
