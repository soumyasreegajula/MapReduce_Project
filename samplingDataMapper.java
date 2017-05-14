package project.MR;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import de.bwaldvogel.liblinear.SolverType;
import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayesMultinomial;
import weka.classifiers.bayes.NaiveBayesMultinomialText;
import weka.classifiers.functions.LibLINEAR;
import weka.classifiers.functions.LibSVM;
import weka.classifiers.functions.LinearRegression;
import weka.classifiers.meta.FilteredClassifier;
import weka.classifiers.trees.RandomForest;
import weka.core.Instances;
import weka.core.SelectedTag;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.filters.Filter;
import weka.filters.unsupervised.attribute.StringToWordVector;
import weka.filters.unsupervised.instance.NonSparseToSparse;
import weka.filters.unsupervised.instance.Resample;

// Mapper program for resampling and building classifiers on the input data
public class samplingDataMapper extends Mapper<Object, Text, NullWritable, BytesWritable> {

	Instances datal;
	int numberofmodels;
	private ObjectOutputStream out = null;

	// Map function to get the samples and to generate models on it
	public void map(Object key, Text value, Context ctx) throws IOException, InterruptedException {
		
		Instances temp = null;

		NonSparseToSparse nsts = new NonSparseToSparse();
		Configuration conf = ctx.getConfiguration();

		FileSystem fs;
		URI[] files = ctx.getCacheFiles();
		// We read all the input partitions after pre processing and add to a
		// single dataset which is resampled for models generation
		try {
			if (files != null) {
				for (int i = 0; i < files.length; i++) {
					CSVLoader loader = new CSVLoader();
					Path sourceFilePath = new Path(files[i]);
					fs = FileSystem.get(sourceFilePath.toUri(), conf);
					FSDataInputStream in = fs.open(sourceFilePath);
					loader.setSource(in);
					loader.setNoHeaderRowPresent(true);
					if (datal == null) {
						datal = loader.getDataSet();
					} else {
						temp = loader.getDataSet();
						datal.addAll(temp);
					}
					in.close();
				}
			} else
				System.out.println("Files NUll\n");
		} catch (Exception e) {
			e.printStackTrace();
		}
		// alternate parallel partitioning is used for generating models.
		Resample r = new Resample();
		r.setRandomSeed((int) (Math.random() * 10000 + 1));
		r.setNoReplacement(false);
		r.setSampleSizePercent(70);
		try {
			r.setInputFormat(datal);
			Instances resampledI = Filter.useFilter(datal, r);

			resampledI.setClassIndex(resampledI.numAttributes() - 1);

			Classifier c = null;
			// Liblinear classifer
			if (value.toString().trim().equalsIgnoreCase("LL")) {
				System.out.println("liblinear");

				LibLINEAR liblinear = new LibLINEAR();
				liblinear.setSVMType(new SelectedTag(LibSVM.SVMTYPE_C_SVC, LibSVM.TAGS_SVMTYPE));
				liblinear.setBias(-1);
				liblinear.setCost(1000); // explicitly high costs to reduce
											// training // error
				liblinear.setEps(1e-3);
				liblinear.setProbabilityEstimates(false);
				liblinear.buildClassifier(resampledI);
				c = liblinear;
			}
			// Random Forest classifier
			if (value.toString().trim().equalsIgnoreCase("RF")) {
				System.out.println("RandomForest");
				RandomForest rf = new RandomForest();
				rf.setNumTrees(15);
				rf.setMaxDepth(4);
				  
				rf.buildClassifier(resampledI);
				c = rf;
			}
			// NaiveBayesMultinomial Classifier
			if (value.toString().trim().equalsIgnoreCase("NBMT")) {
				System.out.println("NaiveBayesMultinomial");

				StringToWordVector stwv = new StringToWordVector();
				stwv.setInputFormat(resampledI);

				FilteredClassifier filteredClassifier = new FilteredClassifier();
				filteredClassifier.setFilter(stwv);
				filteredClassifier.setClassifier(new NaiveBayesMultinomialText());
				filteredClassifier.buildClassifier(resampledI);

				c = filteredClassifier;
			}
			// Storing the models as BytesWritable
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			try {
				out = new ObjectOutputStream(bos);
				out.writeObject(c);
				out.flush();
				byte[] data = bos.toByteArray();
				BytesWritable bytes = new BytesWritable(data);
				ctx.write(NullWritable.get(), bytes);
			} finally {
				bos.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
