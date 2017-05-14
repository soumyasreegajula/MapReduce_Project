package project.MR;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
// Mapper to retieve the seen and not seen attributes and write to the reducer
public class AccuracyDataMapper extends Mapper<IndexKeyWritable, Text, IndexKeyWritable, IntWritable> {
	public void map(IndexKeyWritable key, Text value, Context ctx) throws IOException, InterruptedException {

		String[] attributes = value.toString().split(",");
		String Actual = attributes[attributes.length - 1].trim();
		if (Actual.equalsIgnoreCase("SEEN"))
			ctx.write(key, new IntWritable(1));
		if (Actual.equalsIgnoreCase("NOTSEEN"))
			ctx.write(key, new IntWritable(0));

	}
}
