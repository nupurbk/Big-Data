package A1.NUPUR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class AttributeVectorReduce extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
	{
		Map<String,Integer> attributeVec=new HashMap<String,Integer>();
		String tfidf=null;
		String result=null;
		
		for(Text val :values)
		{
			String[] wordtfid=val.toString().split("\t",4);
			tfidf=wordtfid[0]+"\t"+wordtfid[1]+"\t"+wordtfid[2]+"\t"+wordtfid[3];
			//result=result+"\t"+tfidf;
			//if(!attributeVec.containsKey(key))
				attributeVec.put(tfidf,1);
		}
		
		for(String results:attributeVec.keySet())
		{
		
		context.write(key, new Text(results));
		}
	}



}

