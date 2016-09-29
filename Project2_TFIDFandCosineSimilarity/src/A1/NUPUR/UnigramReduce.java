//Input from mapper(key=author&word),[1,1,1,.....])
//output((key=author&word),value=totalcount -words per author
//First Job

/* Written By -Nupur Kulkarni*/




package A1.NUPUR;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UnigramReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
	
	public void reduce(Text key,Iterable<IntWritable> values,Context context)throws IOException,InterruptedException
	{
		
		int count=0;
		
		for (IntWritable val : values){
			
			
			count+=val.get();
			
		}
		
		 context.write(key, new IntWritable(count));
	}

}