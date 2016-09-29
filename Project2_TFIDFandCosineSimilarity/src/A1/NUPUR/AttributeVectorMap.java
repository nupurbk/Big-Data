//Input :word, author , nij, tf
//output :author word tfidf


package A1.NUPUR;



import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AttributeVectorMap extends Mapper<LongWritable,Text,Text,Text>{

	private String[] wordauthornijtf=null;
	private String author=null;
	private String word =null;
	//private String nijvalue=null;
	//private String tfvalue=null;
	private float tf=0;
	private int nij =0;
	private String authorcount  =null;
	private int N=0;
	
	
	public void map(LongWritable key,Text value,Context context) throws IOException ,InterruptedException{

		wordauthornijtf=value.toString().split("\t",4);
		
		author=wordauthornijtf[0];
		word=wordauthornijtf[1];
		
		
		Configuration conf=context.getConfiguration();
		authorcount=conf.get("N").toString();
		N=Integer.parseInt(authorcount);
		
		nij=Integer.parseInt(wordauthornijtf[2]);
		tf=Float.parseFloat(wordauthornijtf[3]);
		
		float df=(float)((float)N/(float)nij);
		
		float idf=(float)(Math.log10(df)/Math.log10(2));
		
		float tfidf=tf*idf;
		
		context.write(new Text(author), new Text(word+"\t"+ tf +"\t"+ idf +"\t"+ tfidf));
		
		}

}
