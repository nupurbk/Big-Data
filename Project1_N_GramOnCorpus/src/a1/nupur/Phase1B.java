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

public class Phase1B {
	
	public static class Map extends
	Mapper<NullWritable, BytesWritable, Text, dataset> {

		Text text = new Text();
		String author = null;
		HashMap<String, Double> containers = new HashMap<String, Double>();

		public void map(NullWritable key, BytesWritable value, Context context)
		throws IOException, InterruptedException {
			dataset dataset1 = new dataset();

	Path filePath = ((FileSplit) context.getInputSplit()).getPath();
	String fileNameString = filePath.getName();
	dataset1.setName(fileNameString);
	String word; 
	String year = null;

	byte[] fileContent = value.getBytes();
	BufferedReader bufReader = new BufferedReader(new StringReader(
			new String(fileContent)));
	String line = null;
	boolean book_start = false;
	while ((line = bufReader.readLine()) != null) {
		if (book_start) {
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {

				word = tokenizer.nextToken().toLowerCase()
						.replaceAll("[^a-zA-Z]", "");
				double count = 0;
				if(containers.containsKey(word+":"+author)){
					count = containers.get(word+":"+author);
					
				}
				containers.put(word+":"+author, count+1);
				

			}

		
		} else if (line.startsWith("Author:")) {
			author = line.substring(line.lastIndexOf(" ") + 1);
			System.out.println("author: " + author);
			dataset1.setAuthor(author);

		} else if (line.endsWith("***")) {
			book_start = true;

		}

	}
	//dataset1.setWordCount(counts);
	//text.set(word+":"+fileNameString);
	//context.write(text, dataset1);
	Iterator it = containers.entrySet().iterator();
    while (it.hasNext()) {
        HashMap.Entry pair = (HashMap.Entry)it.next();
        System.out.println(pair.getKey() + " = " + pair.getValue());
        String wordkey = pair.getKey().toString();
        Text text = new Text(wordkey);
        dataset1.setWordCount((double) pair.getValue());
        context.write(text,dataset1);
        it.remove(); // avoids a ConcurrentModificationException
    }

}

}

public static class Reduce extends
	Reducer<Text, dataset, Text, Text> {
	
	
	List<String> books_index = new ArrayList<String>();
	
	
	public void reduce(Text key, Iterable<dataset> values,
		Context context) throws IOException, InterruptedException {
		Iterator<dataset> iter = values.iterator();
		int sum = 0;
		int index_count =1;
		while (iter.hasNext()) {
			dataset book = iter.next();
			String bName = book.getName();
			if(!books_index.contains(bName)){
				index_count++;
			}
			books_index.add(bName);
			sum += book.getWordCount();
		 }
	
	String[] items =key.toString().split(":");
	Text outKey =new Text(items[0]+"\t\t\t\t"+items[1]);
	Text outValue = new Text( sum +"\t\t\t"+ index_count);
	 context.write(outKey, outValue);

}
}



public static void main(String[] args) throws Exception {

	Job job = new Job();
	job.setJarByClass(Phase1B.class);
	job.setJobName("Phase1B");

	
	WholeFileInputFormat.setInputPaths(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setInputFormatClass(WholeFileInputFormat.class);
	job.setMapperClass(Map.class);
	job.setReducerClass(Reduce.class);
	job.setJarByClass(Phase1B.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(dataset.class);
	//job.setSortComparatorClass(AscendingKeyComparator.class);
	job.setNumReduceTasks(100); 
	System.exit(job.waitForCompletion(true) ? 0 : 1);

}

	

}
