/**
 * vienteDriver.java
 * @author Swapnil Ashtekar <swapashtekar@gmail.com>
 * Dhruva Patil <patil.dhruva@gmail.com>
 * Nupur Kulkarni <nupurbkulkarni@gmail.com>
 * Apr 11, 2016
 */
package TeamViente;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import TeamViente.XmlDriver.*;
import TeamViente.XmlDriver.XmlInputFormat1;
/**
 * @author sashteka
 *
 */
public class vienteDriver {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	
	
	private static final String OUTPUT_PATH1 = "intermediate_output1";
	//private static final String OUTPUT_PATH2 = "intermediate_output2";
	//private static final String OUTPUT_PATH3 = "intermediate_output3";
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf = new Configuration();
		
		if(args.length != 2){
			System.err.println("Usage: vienteDriver <input_file> <output_file>");
			System.exit(2);
		}
		
		
		conf.set("xmlinput.start", "<entry>");
	    conf.set("xmlinput.end", "</entry>");
	    
		Job job = Job.getInstance(conf, "Viente Big Data Project");
		job.setJarByClass(vienteDriver.class);

		FileInputFormat.setInputDirRecursive(job, true);

		//job.setMapperClass(vienteMapper1.class);
		job.setMapperClass(vienteMapper1.class);
		// job.setCombinerClass(vienteReducer1.class);
		job.setReducerClass(vienteReducer1.class);

		job.setInputFormatClass(XmlInputFormat1.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH1));

		job.waitForCompletion(true);
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		//Second job
		
		Job job2 = Job.getInstance(conf, "Viente Big Data Project 2");
		job2.setJarByClass(vienteDriver.class);

		FileInputFormat.setInputDirRecursive(job2, true);

		//job2.setMapperClass(vienteMapper2.class);
		job2.setMapperClass(vienteMapper2.class);
		job2.setReducerClass(vienteReducer2.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH1));
		FileOutputFormat.setOutputPath(job2, new Path(args[1]));

		//job2.waitForCompletion(true);
		System.exit(job2.waitForCompletion(true) ? 0 : 1);

	}

}
