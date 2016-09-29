package A1.COSINE;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;

public class CosineRed extends Reducer<Text,Text,Text,Text>{

	float product;
	float vectorKnown;
	float vectorUnknown;
	float results;
	float denomi;
	public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException
	{

		for (Text val : values){

			String[] data=val.toString().split("\t",3);
			float TfidKnown=Float.parseFloat(data[1]);
			float TfidUnknown=Float.parseFloat(data[2]);

			product=product+(TfidKnown*TfidUnknown);
			vectorKnown=vectorKnown+(float)Math.pow(TfidKnown, 2);
			vectorUnknown=vectorUnknown+(float)Math.pow(TfidUnknown, 2);
		}

		denomi=(float)(Math.sqrt(vectorKnown)*Math.sqrt(vectorUnknown));
		results=product/denomi;
		context.write(key, new Text(String.valueOf(results)));

	}


}


