package a1.nupur;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
	private FileSplit fileSplit;
	private Configuration conf;
	private BytesWritable value = new BytesWritable();
	private boolean processed = false;


	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public NullWritable getCurrentKey() throws IOException, InterruptedException{
	    return NullWritable.get();
	} 
	

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed ? 1.0f : 0.0f;
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		this.fileSplit = (FileSplit) split;
	    this.conf = context.getConfiguration();
		
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed){
	        byte[] contents = new byte[(int) fileSplit.getLength()];
	        Path file = fileSplit.getPath();
	        FileSystem fs = file.getFileSystem(conf);
	        FSDataInputStream in = null;
	        try{
	            in = fs.open(file);
	            IOUtils.readFully(in, contents, 0, contents.length);
	            value.set(contents, 0, contents.length);
	        }finally{
	            IOUtils.closeStream(in);
	        }
	        processed = true;
	        return  true;
	    }
	    return false;
	}
	
}

