package A1.NUPUR;

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
	
	private static final String OUTPUT_PATH1="OUTPUTJOB1";
	private static final String OUTPUT_PATH2="OUTPUTJOB2";
	private static final String OUTPUT_PATH3="OUTPUTJOB3";
	
	public static void main(String[] args)throws Exception{
		Configuration conf =new Configuration();
		
		Job job=Job.getInstance(conf);
		job.setJarByClass(Main.class);
		
		job.setMapperClass(UnigramMap.class);
		job.setReducerClass(UnigramReduce.class);
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
		
		job1.setMapperClass(TFIDMap.class);
		job1.setReducerClass(TFIDReducer.class);
		
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job1, new Path(OUTPUT_PATH1));
		FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH2));

		if(job1.waitForCompletion(true))
			System.out.println("Job Completed Successfully");
		
		Counters counters=job1.getCounters();
		Counter counter =counters.findCounter(TestCounters.TEST);
		
		long authorCount=counter.getValue();
			     
		conf.set("N",String.valueOf(authorCount));
		
		//JOB3
		Job job2=Job.getInstance(conf);
		
		job2.setJarByClass(Main.class);
		
		job2.setMapperClass(TFIDFMap.class);
		job2.setReducerClass(TFIDFReduce.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job2, new Path(OUTPUT_PATH2));
		FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH3));
		
		

		if(job2.waitForCompletion(true))
			System.out.println("Job Completed Successfully");
		
		//JOB4
		Job job4=Job.getInstance(conf);
		job4.setJarByClass(Main.class);
		
		job4.setMapperClass(AttributeVectorMap.class);
		job4.setReducerClass(AttributeVectorReduce.class);
		job4.setOutputKeyClass(Text.class);
		job4.setOutputValueClass(Text.class);
		job4.setInputFormatClass(TextInputFormat.class);
		job4.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job4, new Path(OUTPUT_PATH3));
		FileOutputFormat.setOutputPath(job4, new Path(args[1]));
		
		

		if(job4.waitForCompletion(true))
			System.out.println("Job Completed Successfully");
	}

}