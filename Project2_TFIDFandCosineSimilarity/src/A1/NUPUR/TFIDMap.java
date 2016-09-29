//Input from mapper(key=author&word),total)
//output((key=author),value=word totalcount -words per author
//First Job

/* Written By -Nupur Kulkarni*/



package A1.NUPUR;


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFIDMap extends Mapper<LongWritable,Text,Text,Text>{

	private String[] authorwordcount=null;
	private String authorword=null;
	

	public void map(LongWritable key,Text value,Context context) throws IOException ,InterruptedException{

		authorwordcount=value.toString().split("\t",3);
		authorword=authorwordcount[1];
		
		context.write(new Text(authorwordcount[0]), new Text(authorword +"=" +authorwordcount[2]));
		
		}










	}

