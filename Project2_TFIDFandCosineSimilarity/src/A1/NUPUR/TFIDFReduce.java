//Input from mapper((key=author & word),value=word  tf
//output((key=word),value=author nij-noofauthorswordappearin, tf
//First Job

/* Written By -Nupur Kulkarni*/



package A1.NUPUR;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class TFIDFReduce extends Reducer<Text,Text,Text,Text> {

	public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
	{
		int nij=0; //number of authors for whom word appears
		
		String tf =null;
		String N=null;
		String author=null;
		String[] wordtf=null;
		Map<String,String>hashset=new HashMap<String,String>();
		//Map<String,Integer>authorset=new HashMap<String,Integer>();
		
		for(Text val:values){

			wordtf=val.toString().split("\t",5);
			nij++;
			String value=wordtf[1]+"\t";
			hashset.put(wordtf[0],value);
			
		}
		
		

		
		

		
		for(String wordaskey :hashset.keySet())
		{
			author=wordaskey;
			tf=hashset.get(wordaskey);
			
			
			context.write(new Text(author), new Text(key+"\t"+nij+"\t"+tf));
			
			
			
		}
		//context.write(new Text(key), new Text(word+"\t"+nij+"\t"+tf));
		
		


	}



}
