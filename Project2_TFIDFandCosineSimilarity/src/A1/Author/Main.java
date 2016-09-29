package A1.Author;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

//import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import A1.NUPUR.TFIDReducer.TestCounters;

public class Main {
	
	private static final String OUTPUT_PATH1="/UNKNOWN/OUTPUTJOB1";
	private static final String OUTPUT_PATH2="/UNKNOWN/OUTPUTJOB2";
	
	public static void main(String[] args)throws Exception{
		Configuration conf =new Configuration();
		
		Job job=Job.getInstance(conf);
		job.setJarByClass(Main.class);
		
		job.setMapperClass(unknownMP1.class);
		job.setReducerClass(unknownRED1.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH1));

		if(job.waitForCompletion(true))
			System.out.println("Job Completed Successfully");
		//job.waitForCompletion(true);
		
		//jOB2
		Job job1=Job.getInstance(conf);
		job1.setJarByClass(Main.class);

		
		job1.setJarByClass(Main.class);
		
		job1.setMapperClass(TFMP.class);
		job1.setReducerClass(TFRE.class);
		
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job1, new Path(OUTPUT_PATH1));
		FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH2));

		if(job1.waitForCompletion(true))
			System.out.println("Job Completed Successfully");
		
	}
}
