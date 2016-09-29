/**
 * vienteMapper1.java
 * @author Swapnil Ashtekar <swapashtekar@gmail.com>
 * Dhruva Patil <patil.dhruva@gmail.com>
 * Nupur Kulkarni <nupurbkulkarni@gmail.com>
 * Apr 11, 2016
 */
package TeamViente;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamReader;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

//import TeamViente.XmlDriver;

public class vienteMapper1 extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper.Context context)
			throws IOException, InterruptedException {
		String document = value.toString();
		System.out.println("‘" + document + "‘");
		try {
			XMLStreamReader reader = XMLInputFactory.newInstance()
					.createXMLStreamReader(
							new ByteArrayInputStream(document.getBytes()));
			String propertyName = "";
			String propertyValue = "";
			//String hostId = "";
			//String hostName = "";
			String currentElement = "";
			String availability30 = "";
			String availability60 = "";
			String availability90 = "";
			String availability365 = "";
			String numberOfReviews = "";
			String reviewScoresRating = "";
			String requiresLicense = "";

			while (reader.hasNext()) {
				int code = reader.next();
				switch (code) {
				case XMLStreamConstants.START_ELEMENT: // START_ELEMENT:
					currentElement = reader.getLocalName();
					break;
				case XMLStreamConstants.CHARACTERS: // CHARACTERS:
					if (currentElement.equalsIgnoreCase("id")) {
						propertyName += reader.getText();
					}
					/*
					 * else if (currentElement.equalsIgnoreCase("host_id")) {
					 * hostId += reader.getText(); } else if
					 * (currentElement.equalsIgnoreCase("host_name")) { hostName
					 * += "\t" + reader.getText(); }
					 */

					else if (currentElement.equalsIgnoreCase("availability_30")) {
						availability30 += "\t" + reader.getText();
					} else if (currentElement
							.equalsIgnoreCase("availability_60")) {
						availability60 += "\t" + reader.getText();
					} else if (currentElement
							.equalsIgnoreCase("availability_90")) {
						availability90 += "\t" + reader.getText();
					} else if (currentElement
							.equalsIgnoreCase("availability_365")) {
						availability365 += "\t" + reader.getText();
					} else if (currentElement
							.equalsIgnoreCase("number_of_reviews")) {
						numberOfReviews += "\t" + reader.getText();
					} else if (currentElement
							.equalsIgnoreCase("review_scores_rating")) {
						reviewScoresRating += "\t" + reader.getText();
					} else if (currentElement
							.equalsIgnoreCase("requires_license")) {
						requiresLicense += "\t" + reader.getText();
					}
					break;
				}
			}
			reader.close();
			/*
			 * propertyValue = hostId.trim() + "\t" + hostName.trim() + "\t" +
			 * availability30.trim() + "\t" + availability60.trim() + "\t" +
			 * availability90.trim() + "\t" + availability365.trim() + "\t" +
			 * numberOfReviews.trim() + "\t" + reviewScoresRating.trim();
			 */

			propertyValue = availability30.trim() + "\t"
					+ availability60.trim() + "\t" + availability90.trim()
					+ "\t" + availability365.trim() + "\t"
					+ numberOfReviews.trim() + "\t" + reviewScoresRating.trim()
					+ "\t" + requiresLicense.trim() ;

			context.write(new Text(propertyName.trim()),
					new Text(propertyValue));

		} catch (Exception e) {
			throw new IOException(e);

		}

	}
}
