package A1.NUPUR;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
//import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

//import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Main1 {
	
	public static void main(String[] args)throws Exception{
		
		Configuration conf =new Configuration();
		
		Job job=Job.getInstance(conf);
		job.setJarByClass(Main1.class);
		
		job.setMapperClass(TFIDMap.class);
		job.setReducerClass(TFIDReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//DistributedCache.addLocalFiles(new Path(args[0]).toUri(), job.getConfiguration());
		//DistributedCache.addLocalFiles(new Path(args[1]).toUri(), job.getConfiguration());
		
		FileInputFormat.setInputPaths(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3]));

		if(job.waitForCompletion(true))
			System.out.println("Job Completed Successfully");
		
	}

}