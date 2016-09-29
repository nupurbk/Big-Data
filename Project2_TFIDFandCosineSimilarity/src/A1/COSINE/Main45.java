

package A1.COSINE;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.filecache.DistributedCache;
//import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;






public class Main45 {
	//@SuppressWarnings("deprecation")	
	public static void main(String[] args)throws Exception{

		Configuration conf =new Configuration();

		Job job=Job.getInstance(conf);
		job.setJarByClass(Main45.class);

		job.setMapperClass(top10Map.class);
		job.setCombinerClass(top10Combiner.class);
		//job.setReducerClass(top10Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//DistributedCache.addCacheFile(new Path(args[2]).toUri(), job.getConfiguration());
		//DistributedCache.addLocalFiles(new Path(args[1]).toUri(), job.getConfiguration());

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//job.setNumReduceTasks(0);
		if(job.waitForCompletion(true))
			System.out.println("Job Completed Successfully");


	}


}




