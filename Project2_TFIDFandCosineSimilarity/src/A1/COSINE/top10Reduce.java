package A1.COSINE;

import java.io.IOException;


import java.util.TreeMap;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class top10Reduce extends Reducer<Text,Text,Text,Text> {
	
	
	public static TreeMap < Float, String> repToRecordMap = new TreeMap < Float,String >();
	public void reduce( Text key, Iterable < Text > values, Context context) throws IOException, InterruptedException {

		for (Text value : values) 
		{
			String[] parsed =  value.toString().split("\t",2);
			repToRecordMap.put(Float.parseFloat(parsed[1]) ,parsed[0].toString());
			// If we have more than ten records, remove the one with the lowest rep
			// As this tree map is sorted in descending order, the user with
			// the lowest reputation is the last key.
			if (repToRecordMap. size() > 10) {
				repToRecordMap.remove(repToRecordMap.firstKey());
			}
		}
		for (String t : repToRecordMap.descendingMap().values()) {
			// Output our ten records to the file system with a null key
			context.write(new Text(" "),new Text(t));
		}
	}
	 
	

       
}
