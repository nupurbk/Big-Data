//Input from mapper((key=author & word),value=word totalcount -words per author,max, tf
//output((key=word),value=author  totalcount-words per author,max, tf
//First Job

/* Written By -Nupur Kulkarni*/



package A1.NUPUR;

//author word count

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFIDFMap extends Mapper<LongWritable,Text,Text,Text>{

	private String[] authorwordcount=null;
	private String word=null;
	private String author=null;
	private String tf=null;
	private String authorcount=null;
	
	public void map(LongWritable key,Text value,Context context) throws IOException ,InterruptedException{

		authorwordcount=value.toString().split("\t",5);
		word=authorwordcount[1];
		author=authorwordcount[0];
		tf=authorwordcount[4];
		
		
		context.write(new Text(word), new Text(author +"\t"+ tf));
		
		}










	}

