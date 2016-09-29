package a1.nupur;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import a1.nupur.WholeFileInputFormat;
import a1.nupur.dataset;

public class Phase2A {
	public static class Map extends
			Mapper<NullWritable, BytesWritable, Text, dataset> {

		Text text = new Text();
		String author = null;
		HashMap<String, Double> hm = new HashMap<String, Double>();
		

		public void map(NullWritable key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			dataset dataset1= new dataset();

			Path filePath = ((FileSplit) context.getInputSplit()).getPath();
			String fileNameString = filePath.getName();
			dataset1.setName(fileNameString);
			String word;
			String year = null;

			byte[] fileContent = value.getBytes();
			BufferedReader bufReader = new BufferedReader(new StringReader(
					new String(fileContent)));
			String line = null;
			boolean read_start = false;
			double endCount = 0;
			String start = new String("_START_");
			String end = new String("_END_");
			
			while ((line = bufReader.readLine()) != null) {
				
				if (read_start) {
					StringTokenizer tokenizer = new StringTokenizer(line);
					while (tokenizer.hasMoreTokens()) {

						word = tokenizer.nextToken().toLowerCase()
								.replaceAll("[^a-zA-Z]", "");
						//word=tokenizer.replaceAll("\\s", "");
						
						if(word.length()!=0)
						{
							String bigram = start + "," + word;
							double count = 0;
							if (hm.containsKey(bigram+ ":" + year)) {
								count = hm.get(bigram+ ":" + year);
							}
							start = word;
							hm.put(bigram + ":" + year, count + 1);
						}

					}
					String bigram2 = start+ "," + end;
					if(hm.containsKey(bigram2 + ":" + year)) {
						endCount = hm.get(bigram2 + ":" + year);

					}
					hm.put(bigram2 + ":" + year, endCount + 1);

				} else if (line.startsWith("Release Date:")) {
					if(line.contains(",")){
			    		int YearStart = line.indexOf(",") + 2;
			    		year = line.substring(YearStart,YearStart+4);
			    	}
			    	else{
			    		int YearStart = line.indexOf(":") + 2;
			    		year = line.substring(YearStart,YearStart+4);
			    	}    

				}  else if (line.endsWith("***")) {
					read_start = true;

				}

			}
			
			Iterator it = hm.entrySet().iterator();
			while (it.hasNext()) {
				HashMap.Entry pair = (HashMap.Entry) it.next();
				System.out.println(pair.getKey() + " = " + pair.getValue());
				String wordkey = pair.getKey().toString();
				Text text = new Text(wordkey);
				dataset1.setWordCount((double) pair.getValue());
				context.write(text, dataset1);
				it.remove(); // avoids a ConcurrentModificationException
			}

		}

	}

	public static class Reduce extends Reducer<Text, dataset, Text, Text> {
		Text result = new Text();
		List<String> books = new ArrayList<String>();
		HashMap<String, String> finalStr = new HashMap<String, String>();

		public void reduce(Text key, Iterable<dataset> values, Context context)
				throws IOException, InterruptedException {
			Iterator<dataset> iter = values.iterator();
			int sum = 0;
			int index_count = 1;
			while (iter.hasNext()) {
				dataset book = iter.next();
				String bName = book.getName();
				if (!books.contains(bName)) {
					index_count++;
				}
				books.add(bName);
				sum += book.getWordCount();
			}
			// dataset record = iter.next();
			String[] items = key.toString().split(":");
			Text outKey = new Text(items[0] + "\t\t\t" + items[1]);
			Text outValue = new Text(sum + "\t\t\t" + index_count);
			context.write(outKey, outValue);

		}
	}

	public static void main(String[] args) throws Exception {

		Job job = new Job();
		job.setJarByClass(Phase2A.class);
		job.setJobName("Phase2A");

		// FileInputFormat.setInputPaths(job, new Path(args[0]));
		WholeFileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setJarByClass(Phase2A.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(dataset.class);
		// job.setSortComparatorClass(KeyComparator.class);
		job.setNumReduceTasks(40);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}