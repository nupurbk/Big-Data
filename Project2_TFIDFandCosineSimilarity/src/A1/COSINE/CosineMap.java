package A1.COSINE;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class CosineMap  extends Mapper <LongWritable,Text,Text,Text>{

	public static Path[] cachefile = new Path[2];
	//@SuppressWarnings("deprecation")
	//@Override
	//private String ch=null;
	public static Map<String,String> author=new HashMap<String,String>();

	//Configuration conf =Context.getConfiguration();

	public void setup(Context context) 
	{
		String[] line=null;  
		Configuration conf = context.getConfiguration();
		try{
			cachefile = DistributedCache.getLocalCacheFiles(conf);
			BufferedReader reader = new BufferedReader(new FileReader(cachefile[0].toString())); 
			String lineUn = reader.readLine();
			while(lineUn!=null)
			{
				line=lineUn.split("\t",3);
				String UnknownWord=line[1].toString();
				String Unknowntf=line[2].toString();

				author.put(UnknownWord, Unknowntf);
				lineUn = reader.readLine();
			}

		}
		catch (IOException e) 
		{
			e.printStackTrace();
		}


	}



	public void map(LongWritable key,Text value,Context context) throws IOException ,InterruptedException{

		//@SuppressWarnings("resource")
		//Configuration conf = context.getConfiguration();
		// try{
		//cachefile = DistributedCache.getLocalCacheFiles(conf);
		// }
		// catch (IOException e) 
		// {
		//e.printStackTrace();
		//}
		//BufferedReader reader = new BufferedReader(new FileReader(cachefile[0].toString())); 

		//String[] line=null;
		String[] attributevector=value.toString().split("\t",5);
		String keys=attributevector[0];
		String word=attributevector[1];
		//String tf=attributevector[2];
		String idfknown=attributevector[3];
		String tfidf=attributevector[4];
		float tfidfknown=Float.parseFloat(tfidf);

		float tfidfUnknown=0;
		//String lineUn = reader.readLine();
		//while(lineUn!=null)
		//{
		//line=lineUn.split("\t",3);
		//String UnknownWord=line[1].toString();
		//String Unknowntf=line[2].toString();
		if(author.containsKey(word))
		{
			tfidfUnknown=Float.parseFloat(idfknown)*Float.parseFloat(author.get(word));
		}

		//lineUn = reader.readLine();

		//}

		/*	else
			{
				tfidfUnknown=Float.parseFloat(idfknown)*(float)(Math.log10(56)/Math.log10(2));
			}*/


		context.write(new Text(keys), new Text(word+"\t"+tfidfknown+"\t"+tfidfUnknown));
		//public void main(


	}
}
