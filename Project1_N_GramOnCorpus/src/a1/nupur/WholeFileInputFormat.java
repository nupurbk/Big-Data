package a1.nupur;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WholeFileInputFormat extends
		FileInputFormat<NullWritable, BytesWritable> {
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}



	@Override
	public RecordReader<NullWritable, BytesWritable> createRecordReader(
			InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		WholeFileRecordReader reader = new WholeFileRecordReader();
		reader.initialize(inputSplit, context);

		return reader;
	}

}