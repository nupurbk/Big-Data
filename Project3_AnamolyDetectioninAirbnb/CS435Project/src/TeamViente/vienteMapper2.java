package TeamViente;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class vienteMapper2 extends Mapper<LongWritable, Text, Text, Text> {

	String label = "";
	String finalString = "";
	int labelint = 0;

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString().replaceAll("\"", "");
		//String line = value.toString();
		String values[] = line.toString().split("\t", 0);

		String listingId = values[0];
		
		/*
		int availability30 = Integer.parseInt(values[1]);
		int availability60 = Integer.parseInt(values[2]);
		int availability90 = Integer.parseInt(values[3]);
		int availability365 = Integer.parseInt(values[4]);
		int numberOfReviews = Integer.parseInt(values[5]);
		*/
		
		String availability30 = values[1];
		String availability60 = values[2];
		String availability90 = values[3];
		String availability365 = values[4];
		String numberOfReviews = values[5];
		String reviewScoreRatings = values[6];
		String requiresLicense = values[7];
		

		String reviewscoreratings = values[6];

		// if NoofReviews and review score ratings are zero

		if (numberOfReviews.equals("0") || numberOfReviews.equals("1")) {
			// if availability over 30 ,60 90 and 365 days is 0-fraud
			if (availability30.equals("0") && availability60.equals("0") && availability90.equals("0")
					&& availability365.equals("0")) {

				label = "Fraud";
				labelint = 0;

			}
			// if availability is full-fraud
			else if (availability30.equals("30") && availability60.equals("60") && availability90.equals("90")
					&& availability365.equals("365")) {

				label = "Fraud";
				labelint = 0;

			}

			// if availability over 30 days is more than 25,over 60 days is
			// more than 50 and availability over 90 days is more than 75
			// for 365 days if its more than 275-fraud
			else if (Integer.parseInt(availability30) >= 25 && Integer.parseInt(availability60) >= 50
					&& Integer.parseInt(availability90) >= 75 && Integer.parseInt(availability365) >= 275) {

				label = "Fraud";
				labelint = 0;
			}
			// label=0;

		}
		else if(availability30.equals("30") && availability60.equals("60") && availability90.equals("90")
				&& availability365.equals("365") && Integer.parseInt(numberOfReviews) < 5) {

			label = "Fraud";
			labelint = 0;

		}
		
		/*else if(Integer.parseInt(availability90) >= 85 && Integer.parseInt(availability365) >= 300) {

			label = "Fraud";
			labelint = 0;
		}*/

		else {
			label = "NotFraud";
			labelint = 1;
		}
		
		//Context write after checking conditions for labels

		String finalString = availability30 + "\t" + availability60 + "\t" + availability90 + "\t" + availability365
				+ "\t" + numberOfReviews + "\t" + labelint;
		context.write(new Text(listingId), new Text(finalString));
		

		
//Previous code is below with less conditions
		
		/*if (availability30 < 30 && availability60 < 50 && availability90 < 90
				&& availability365 < 350 && numberOfReviews == 0) {

			label = "Fraud";
			labelint = 0;
			// label=0;

		}
		

		else {
			label = "NotFraud";
			labelint = 1;
		}

		
		String finalString = availability30 + "\t" + availability60 + "\t"
				+ availability90 + "\t" + availability365 + "\t"
				+ numberOfReviews + "\t" + reviewScoreRatings + "\t"
				+ requiresLicense + "\t" + labelint;

		context.write(new Text(listingId), new Text(finalString));
		*/
	}
}