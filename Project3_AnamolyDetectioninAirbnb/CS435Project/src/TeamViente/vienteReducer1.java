/**
 * vienteReducer1.java
 * @author Swapnil Ashtekar <swapashtekar@gmail.com>
 * Dhruva Patil <patil.dhruva@gmail.com>
 * Nupur Kulkarni <nupurbkulkarni@gmail.com>
 * Apr 11, 2016
 */
package TeamViente;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * @author sashteka
 *
 */
public class vienteReducer1 extends Reducer<Text, Text, Text, Text> {

	/**
	 * @param args
	 */

	/*
	 * @Override protected void setup( Context context) throws IOException,
	 * InterruptedException { context.write(new Text("<csvdata>"), null); }
	 * 
	 * @Override protected void cleanup( Context context) throws IOException,
	 * InterruptedException { context.write(new Text("</csvdata>"), null); }
	 */

	private Text outputKey = new Text();

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		for (Text value : values) {
			// outputKey.set(constructPropertyXml(key, value));
			
			String[] totalLine = value.toString().split("\t",0);
			
			String availability30 = totalLine[0];
			String availability60 = totalLine[1];
			String availability90 = totalLine[2];
			String availability365 = totalLine[3];
			String numberOfReviews = totalLine[4];
			String reviewScoresRating = totalLine[5];
			String requiresLicense = totalLine[6]; 
			
			if(reviewScoresRating.contains("\"\"")){
				reviewScoresRating = "\"0\"";
			}
			
			String OutputValue = availability30.trim() + "\t"
					+ availability60.trim() + "\t" + availability90.trim()
					+ "\t" + availability365.trim() + "\t"
					+ numberOfReviews.trim() + "\t" + reviewScoresRating.trim()
					+ "\t" + requiresLicense.trim() ;

			context.write(new Text(key.toString().replace("\"", "")), new Text(OutputValue));
		}
	}

	public static String constructPropertyXml(Text name, Text value) {
		StringBuilder sb = new StringBuilder();
		sb.append("id: ").append(name).append("hostname: ").append(value).append("\n");
		return sb.toString();
	}

}
