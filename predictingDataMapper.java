package project.MR;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import weka.classifiers.Classifier;
import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;
// Mapper for predicting the bird results based on the models classfied on the unlabelled data
public class predictingDataMapper extends Mapper<IndexKeyWritable, Text, IndexKeyWritable, IntWritable> {

	Instances datal;
	int numberofmodels;
	private Classifier models[];
	private Instances dataset = null;
	private int index = 0;
	// set up function to get all the models that are persisted
	public void setup(Context ctx) throws IOException {
		models = new Classifier[10];

		URI[] files = ctx.getCacheFiles();
		try {
			if (files != null) {
				for (int i = 0; i < files.length; i++) {
					byte[] input = null;
					SequenceFile.Reader reader = new SequenceFile.Reader(ctx.getConfiguration(),
							SequenceFile.Reader.file(new Path(files[i])));

					BytesWritable baw = new BytesWritable();
					while (reader.next(NullWritable.get(), baw)) {

						input = baw.getBytes();
					}
					reader.close();
					ByteArrayInputStream bis = new ByteArrayInputStream(input);
					ObjectInputStream in2 = new ObjectInputStream(bis);
					models[i] = (Classifier) in2.readObject();

					in2.close();
					bis.close();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	// Map function to do the prediction on each record of the unlabelled data based on the models classified 
	@SuppressWarnings("unchecked")
	public void map(IndexKeyWritable key, Text value, Context ctx) throws IOException, InterruptedException {

		if (index == 0) {

			String[] attributes = value.toString().split(",");
			ArrayList<Attribute> atts = new ArrayList<Attribute>();

			for (int att = 1; att < attributes.length; att++) {
				atts.add(new Attribute("att" + att, att - 1));
			}

			@SuppressWarnings("rawtypes")
			List my_nominal_values = new ArrayList(2);
			// asiiging "seen" and "notseen" nominal values
			my_nominal_values.add("NOTSEEN");
			my_nominal_values.add("SEEN");
			atts.add(new Attribute(new String("att").concat(String.valueOf(attributes.length)), my_nominal_values));
			
			dataset = new Instances("testdata", atts, 0);
			dataset.setClassIndex(dataset.numAttributes() - 1);

			index++;
		}

		String[] attributes = value.toString().split(",");
		Instance inst = new DenseInstance(attributes.length);
		inst.setDataset(dataset);
		for (int i = 0; i < attributes.length - 1; i++)
			inst.setValue(i, Float.parseFloat(attributes[i]));
		inst.setClassMissing();
		int seen = 0;
		//Horizontal stripes design pattern for prediction.

		for (Classifier model : models) {

			try {
				double result = model.classifyInstance(inst);
				if (dataset.classAttribute().value((int) result).trim().equalsIgnoreCase("SEEN"))
					seen++;

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		if (seen > 5)
			ctx.write(key, new IntWritable(1));
		else
			ctx.write(key, new IntWritable(0));

	}
}