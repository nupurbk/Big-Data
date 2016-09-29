package A1.Author;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TFMP extends Mapper<LongWritable,Text,Text,Text>{

	private String[] authorwordcount=null;
	private String authorword=null;
	

	public void map(LongWritable key,Text value,Context context) throws IOException ,InterruptedException{

		authorwordcount=value.toString().split("\t",3);
		authorword=authorwordcount[1];
		
		context.write(new Text(authorwordcount[0]), new Text(authorword +"=" +authorwordcount[2]));
		
		}

}