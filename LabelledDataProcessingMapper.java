package project.MR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import project.MR.AttribEnum.Attributes;

// The preprocessing program for Labelled data
public class LabelledDataProcessingMapper extends Mapper<Object, Text, NullWritable, Text> {

	private int count;
	// List of all the protocols
	String[] protocol_array = { "P20", "P21", "P22", "P23", "P34", "P35", "P39", "P40", "P41", "P44", "P45", "P46",
			"P47", "P48", "P49", "P50", "P51", "P52", "P55", "P56" };
	ArrayList<String> protocol_list;
	private String columns[];
	String output;
	private long taskID;

	public enum COUNTERS {
		PARTITIONS_LABELLED, BIRD_NOT_VALID
	}

	// Function to get the status/value of the target bird
	public boolean Agelaius_phoeniceus_isValid(String value) {
		if (value.equals("?"))
			return false;
		else
			return true;
	}

	// If the value of target column is 1 or ? it is grouped as true
	private boolean isGroupData(String value) {

		if (value.equalsIgnoreCase("1") || value.equalsIgnoreCase("?"))
			return true;
		else
			return false;
	}

	// main process of splitting the file and retrieving the data for further
	// processing
	public void setup(Context ctx) {
		count = 0;
		taskID = (long) ctx.getTaskAttemptID().getTaskID().getId();
		protocol_list = new ArrayList<String>(Arrays.asList(protocol_array));
		output = new String();
		ctx.getCounter(COUNTERS.PARTITIONS_LABELLED).increment(1L);
		String paths[] = ((FileSplit) ctx.getInputSplit()).toString().split(":");
		String splitinfo = paths[paths.length - 1];
		int index = splitinfo.indexOf('+');
		if (index >= 0)
			taskID = Long.parseLong(splitinfo.substring(0, index).trim());
	}

	// Map function for the pre processing jobs as mentioned in the report
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		if ((taskID == 0) && (count == 0)) {

			count++; // To skip Headers in the file
		} else {
			output = "";
			columns = value.toString().split(",");
			Attributes bird = Attributes.AGELAIUS_PHOENICEUS;
			if (!Agelaius_phoeniceus_isValid(columns[bird.ordinal()].trim())) {
				context.getCounter(COUNTERS.BIRD_NOT_VALID).increment(1L);
				return;
			}

			Attributes group = Attributes.PRIMARY_CHECKLIST_FLAG;
			if (!isGroupData(columns[group.ordinal()].trim())) {
				context.getCounter(COUNTERS.BIRD_NOT_VALID).increment(1L);
				return;
			}
			// Concataning time as 6 hour time slot
			Attributes time = Attributes.TIME;
			settimearray(columns[time.ordinal()]);
			// Imputing latitude and longitude in a xyz plane
			Attributes latitude = Attributes.LATITUDE;
			Attributes longitude = Attributes.LONGITUDE;
			float R = 6371;
			double lat = Double.valueOf(columns[latitude.ordinal()]);
			double lon = Double.valueOf(columns[longitude.ordinal()]);
			float x = (float) (R * Math.cos(lat) * Math.cos(lon));
			float y = (float) (R * Math.cos(lat) * Math.sin(lon));
			float z = (float) (R * Math.sin(lat));
			output = output.concat(String.valueOf(x) + ",");
			output = output.concat(String.valueOf(y) + ",");
			output = output.concat(String.valueOf(z) + ",");
			// Handling protocols columns
			Attributes protocoltype = Attributes.COUNT_TYPE;
			String protocol = columns[protocoltype.ordinal()];
			String protovalues[] = new String[20];
			Arrays.fill(protovalues, "0.0");
			int protocol_index = protocol_list.indexOf(protocol);
			if (protocol_index >= 0)
				protovalues[protocol_index] = "1.0";
			for (int i = 0; i < protovalues.length; i++)
				output = output.concat(protovalues[i]).concat(",");
			// setting the CAS values,missing values and ? values
			setCAUSvalues();
			// setting the Elevations values,missing values and ? values
			setElevations();
			// setting the Housing values,missing values and ? values
			setHousingPopRecords();
			if (columns[bird.ordinal()].trim().equalsIgnoreCase("0"))
				output = output.concat(",NOTSEEN");
			else {
				output = output.concat(",SEEN");
			}
			Text out = new Text(output);
			context.write(NullWritable.get(), out);

		}
	}

	// setting the Housing values,missing values and ? values
	private void setHousingPopRecords() {
		float housing_density = 0.0f;
		float housing_percent_vacant = 0.0f;
		float population_per_squaremile = 0.0f;
		Attributes hd = Attributes.HOUSING_DENSITY;
		Attributes hpv = Attributes.HOUSING_PERCENT_VACANT;
		Attributes pps = Attributes.POP00_SQMI;
		if (!columns[hd.ordinal()].trim().equals("?"))
			housing_density = Float.parseFloat(columns[hd.ordinal()]);
		if (!columns[hpv.ordinal()].trim().equals("?"))
			housing_percent_vacant = Float.parseFloat(columns[hpv.ordinal()]);
		if (!columns[pps.ordinal()].trim().equals("?"))
			population_per_squaremile = Float.parseFloat(columns[pps.ordinal()]);
		output = output.concat(String.valueOf(housing_density).concat(",").concat(
				String.valueOf(housing_percent_vacant).concat(",").concat(String.valueOf(population_per_squaremile))));
	}

	// setting the Elevations values,missing values and ? values
	private void setElevations() {

		Attributes elev_gt = Attributes.ELEV_GT;
		Attributes elev_ned = Attributes.ELEV_NED;
		float feleve_gt = 0.0f;
		float feleve_ned = 0.0f;
		if (!columns[elev_gt.ordinal()].trim().equals("?"))
			feleve_gt = Float.parseFloat(columns[elev_gt.ordinal()]);
		output = output.concat(String.valueOf(feleve_gt)).concat(",");
		if (!columns[elev_ned.ordinal()].trim().equals("?"))
			feleve_ned = Float.parseFloat(columns[elev_ned.ordinal()]);
		output = output.concat(String.valueOf(feleve_ned)).concat(",");
	}

	// setting the CAS values,missing values and ? values
	private void setCAUSvalues() {
		Attributes caus_prec = Attributes.CAUS_PREC;
		if (columns[caus_prec.ordinal()].trim().equals("?"))
			output = output.concat("0.0").concat(",");
		else
			output = output.concat(columns[caus_prec.ordinal()].trim()).concat(",");

		Attributes caus_snow = Attributes.CAUS_SNOW;
		if (columns[caus_snow.ordinal()].trim().equals("?"))
			output = output.concat("0.0").concat(",");
		else
			output = output.concat(columns[caus_snow.ordinal()].trim()).concat(",");
		Attributes caus_temp_avg = Attributes.CAUS_TEMP_AVG;
		if (columns[caus_temp_avg.ordinal()].trim().equals("?"))
			output = output.concat("0.0").concat(",");
		else
			output = output.concat(columns[caus_temp_avg.ordinal()].trim()).concat(",");
		Attributes caus_temp_min = Attributes.CAUS_TEMP_MIN;
		if (columns[caus_temp_min.ordinal()].trim().equals("?"))
			output = output.concat("0.0").concat(",");
		else
			output = output.concat(columns[caus_temp_min.ordinal()].trim()).concat(",");
		Attributes caus_temp_max = Attributes.CAUS_TEMP_MAX;
		if (columns[caus_temp_max.ordinal()].trim().equals("?"))
			output = output.concat("0.0").concat(",");
		else
			output = output.concat(columns[caus_temp_max.ordinal()].trim()).concat(",");

	}

	// Concataning time as 6 hour time slot
	private void settimearray(String time) {

		float ftime = Float.valueOf(time).floatValue();
		float timeslots[] = new float[4];
		if (ftime >= 3 && ftime <= 8)
			timeslots[0] = 1.0f;
		else
			timeslots[0] = 0.0f;
		if (ftime <= 15 && ftime >= 8)
			timeslots[1] = 1.0f;
		else
			timeslots[1] = 1.0f;
		if (ftime >= 15 && ftime <= 20)
			timeslots[2] = 1.0f;
		else
			timeslots[2] = 1.0f;
		if (ftime >= 20 || ftime <= 3)
			timeslots[3] = 1.0f;
		else
			timeslots[3] = 1.0f;
		for (int i = 0; i < timeslots.length; i++)
			output = output.concat(String.valueOf(timeslots[i]).concat(","));

	}

}