package project.MR;

// Import the required libraries
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import project.MR.LabelledDataProcessingMapper.COUNTERS;

// The main program
public class BirdPrediction {

	private static long cachepartitions;
	private static String[] otherArgs;

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// Parameters authentication
		if (otherArgs.length < 2) {
			System.err.println("Usage: Project <in> [<in>...] <out>");
			System.exit(2);
		}
		// Job for preprocessing UnLabelled data
		Job job2 = Job.getInstance(conf, "Data_Cleaning UnLabelled Data");
		job2.setJarByClass(BirdPrediction.class);
		job2.setMapperClass(UnLabelledDataProcessingMapper.class);
		job2.setNumReduceTasks(0);
		job2.setMapOutputKeyClass(IndexKeyWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(IndexKeyWritable.class);
		job2.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job2, new Path(otherArgs[i], "RawUnLabelledData"));
		}
		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[otherArgs.length - 1], "ProcessedUnLabelledData"));
		job2.waitForCompletion(true);

		// Job for preprocessing Labelled data
		conf = new Configuration();
		Job job = Job.getInstance(conf, "Data_Cleaning Labelled Data");
		job.setJarByClass(BirdPrediction.class);
		job.setMapperClass(LabelledDataProcessingMapper.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setNumReduceTasks(0);
		job.setMapOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i], "RawLabelledData"));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1], "ProcessedLabelledData"));
		job.waitForCompletion(true);
		cachepartitions = job.getCounters().findCounter(COUNTERS.PARTITIONS_LABELLED).getValue();
		// Configuration settings to avoid memory issue on aws
		conf = new Configuration();

		long milliSeconds = 1000 * 60 * 60;
		conf.setLong("mapred.task.timeout", milliSeconds);
		/*
		 * conf.set("mapreduce.map.memory.mb", "2048");
		 * conf.set("mapreduce.reduce.memory.mb", "1024");
		 * conf.set("mapred.child.java.opts", "-Xmx14096m -Xss2048m") ;
		 * conf.set("mapreduce.map.child.java.opts", "-Xmx8072m") ;
		 * conf.set("mapreduce.reduce.child.java.opts", "-Xmx3024m") ;
		 */
		// The resample and models generation job
		samplingData(otherArgs[0], otherArgs[1], conf);
	}

	// The resample and models generation job
	public static void samplingData(String inputPath, String outputPath, Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {
		Job job = Job.getInstance(conf, "sampling");
		job.setJarByClass(BirdPrediction.class);
		job.setMapperClass(samplingDataMapper.class);
		// We cache the models so that they can be used for prediction
		for (int i = 0; i < cachepartitions; i++) {
			String partfilename = "part-m-00";
			if (i < 10)
				partfilename = partfilename.concat("00").concat(String.valueOf(i));
			if (i >= 10 && i <= 99)
				partfilename = partfilename.concat("0").concat(String.valueOf(i));
			if (i >= 100)
				partfilename = partfilename.concat(String.valueOf(i));
			String path = String.valueOf("ProcessedLabelledData").concat("/").concat(partfilename);
			job.addCacheFile((new Path(outputPath, path).toUri()));

		}
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setNumReduceTasks(0);
		// Nline Input format to implement distributed cache and parallel
		// processing
		job.setInputFormatClass(NLineInputFormat.class);
		NLineInputFormat.setNumLinesPerSplit(job, 1);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(inputPath, "ModelsInput"));
		SequenceFileOutputFormat.setOutputPath(job, new Path(outputPath, "ModelsOutput"));
		job.waitForCompletion(true);

		conf = new Configuration();
		// conf = new Configuration();
		//////////////////////////////////////////////////////
		 long milliSeconds = 1000*60*60*60;
		 conf.setLong("mapred.task.timeout", milliSeconds);
		 conf.set("mapreduce.map.memory.mb", "2048");
		  conf.set("mapreduce.reduce.memory.mb", "1024");
		  conf.set("mapred.child.java.opts", "-Xmx14096m -Xss2048m") ;
		  conf.set("mapreduce.map.child.java.opts", "-Xmx8072m") ;
		  conf.set("mapreduce.reduce.child.java.opts", "-Xmx3024m") ;
		// The job for the prediction process where the models are sent as
		// parameters
		PredictingData(outputPath, outputPath, conf);
	}

	// Group comparator to get the prediction in the same order as that of
	// unlabelled data
	public static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(IndexKeyWritable.class, true);
		}

		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			IndexKeyWritable k1 = (IndexKeyWritable) w1;
			IndexKeyWritable k2 = (IndexKeyWritable) w2;
			return (new Long(k1.gettaskID())).compareTo(new Long(k2.gettaskID()));
		}
	}

	// The job for the prediction process where the models are sent as
	// parameters
	public static void PredictingData(String inputPath, String outputPath, Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {

		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf, "Prediction");
		job.setJarByClass(BirdPrediction.class);
		job.setMapperClass(predictingDataMapper.class);
		job.setReducerClass(predictingDataReducer.class);
		job.setMapOutputKeyClass(IndexKeyWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setGroupingComparatorClass(GroupComparator.class);

		job.setNumReduceTasks(1);
		// persist models
		for (int i = 0; i < 10; i++) {
			String partfilename = "part-m-00";
			if (i < 10)
				partfilename = partfilename.concat("00").concat(String.valueOf(i));
			if (i >= 10 && i <= 99)
				partfilename = partfilename.concat("0").concat(String.valueOf(i));
			if (i >= 100)
				partfilename = partfilename.concat(String.valueOf(i));
			
			String path = String.valueOf("ModelsOutput").concat("/").concat(partfilename);
			job.addCacheFile((new Path(inputPath, path).toUri()));
		}
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(outputPath, "ProcessedUnLabelledData"));
		System.out.println("Input Path " + new Path(outputPath, "ProcessedUnLabelledData").toString());
		FileOutputFormat.setOutputPath(job, new Path(outputPath, "ModelsClassified"));
		job.waitForCompletion(true);
		conf = new Configuration();
		//long milliSeconds = 1000*60*60;
		 //conf.setLong("mapred.task.timeout", milliSeconds);
		// Job to calculate the accuaray of the prediction results
		//AccuracyData(inputPath, outputPath, conf);

	}

	// Job to calculate the accuaray of the prediction results
	public static void AccuracyData(String inputPath, String outputPath, Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {

		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf, "Accuracy");
		job.setJarByClass(BirdPrediction.class);
		job.setMapperClass(AccuracyDataMapper.class);
		job.setReducerClass(AccuracyDataReducer.class);
		job.setMapOutputKeyClass(IndexKeyWritable.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setNumReduceTasks(1);
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(outputPath, "ProcessedUnLabelledData"));
		FileOutputFormat.setOutputPath(job, new Path(outputPath, "AccuracyInputData"));
		job.waitForCompletion(true);
		conf = new Configuration();
		//long milliSeconds = 1000*60*60;
		 //conf.setLong("mapred.task.timeout", milliSeconds);
		ScoringData(otherArgs[0], outputPath, conf);
	}

	// Job to get the score results in the desired format
	public static void ScoringData(String inputPath, String outputPath, Configuration conf)
			throws IOException, ClassNotFoundException, InterruptedException {

		conf.set("mapreduce.output.textoutputformat.separator", ",");
		Job job = Job.getInstance(conf, "Accuracy");
		job.setJarByClass(BirdPrediction.class);
		job.setMapperClass(ScoringMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		job.setGroupingComparatorClass(GroupComparator.class);
		job.setNumReduceTasks(0);
		String path = String.valueOf("ModelsClassified").concat("/").concat("part-r-00000");
		job.addCacheFile((new Path(outputPath, path).toUri()));
		path = String.valueOf("AccuracyInputData").concat("/").concat("part-r-00000");
		job.addCacheFile((new Path(outputPath, path).toUri()));
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(inputPath, "ModelsInput"));
		FileOutputFormat.setOutputPath(job, new Path(outputPath, "AccuracyResult"));
		job.waitForCompletion(true);
	}

}