package a1.nupur;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;



public class dataset implements Writable{
	private String name;
	private double year;
	private String author;
	private double  wordCount;

	public dataset() {
		this.name =new String();
		this.year = new Double(0);
		 this.author = new String();
		 this.wordCount = new Double(0);
	}

	 public dataset(String name, double year, String author, double wordCount) {
	 this.name = name;
	 this.year = year;
	 this.author = author;
	 this.wordCount = wordCount;
	 }
	
	

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Double getYear() {
		return year;
	}

	public void setYear(Double year) {
		this.year = year;
	}

	public String getAuthor() {
		return author;
	}

	public void setAuthor(String author) {
		this.author = author;
	}

	public Double getWordCount() {
		return wordCount;
	}
	

	public void setWordCount(Double wordCount) {
		this.wordCount = wordCount;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		year = in.readDouble();
	    author = in.readUTF();
	    wordCount = in.readDouble();
	    name = in.readUTF();
	    
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(year);
	    out.writeUTF(name);
	    out.writeDouble(wordCount);
	    out.writeUTF(author);
		
	}

	
}


