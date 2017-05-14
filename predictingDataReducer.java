package project.MR;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
// reducer for writing the results to the output files in the same order as that of Unlabelled data
public class predictingDataReducer extends Reducer<IndexKeyWritable, IntWritable, Text, Text> {

	public void setup(Context ctx) throws IOException, InterruptedException {
		
		ctx.write(new Text("SAMPLING_EVENT_ID"),new Text("SAW_AGELAIUS_PHOENICEUS"));
	}
	public void reduce(IndexKeyWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		for(IntWritable value : values)
			context.write(new Text(key.getSamplingID()), new Text(String.valueOf(value.get())));
	}
}