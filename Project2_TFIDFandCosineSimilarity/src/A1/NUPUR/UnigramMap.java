//Input(key=author&word),value=1)
//output((key=author&word),value=totalcount -words per author
//First Job

/* Written By -Nupur Kulkarni*/

package A1.NUPUR;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UnigramMap extends Mapper<LongWritable,Text,Text,IntWritable>{

	private String author =null;
	private String line=null;
	private String keys=null;
	private String[] str=null;
	private Text word=new Text();

	public void map(LongWritable key,Text value,Context context) throws IOException ,InterruptedException{



		if(value.toString().contains("<===>"))
		{
			str = value.toString().split("<===>",2);
			author=str[0].toString().toLowerCase().replaceAll("\\p{P}", "");
		}

		line=str[1].toString().toLowerCase().replaceAll("\\p{P}", "");
		StringTokenizer tokenizer = new StringTokenizer(line.toString()," ");
		while (tokenizer.hasMoreTokens())
		{
			String token=(String)tokenizer.nextToken().toLowerCase()
					.replaceAll("[^a-zA-Z]", "");
			if(token.length() != 0) {
				keys = author + "\t" + token;
				//Buffer = substring;
				word.set(keys);
				context.write(word, new IntWritable(1));;
			}


		}










	}

}