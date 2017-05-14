package project.MR;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
// Scores file given by  professor togenearte accuracy
public class ScoringMapper extends Mapper<Object, Text, Text, DoubleWritable> {
	
	public void setup(Context ctx) throws IOException {

		FileSystem fs;
		Configuration conf = ctx.getConfiguration();
		FSDataInputStream truthstream = null,resultstream = null;
		BufferedReader resultReader = null, truthReader = null;

		URI[] files = ctx.getCacheFiles();
		try {
			if (files != null) {
				for (int i = 0; i < files.length; i++) {
					Path sourceFilePath = new Path(files[i]);
					
					fs = FileSystem.get(sourceFilePath.toUri(), conf);
					if(sourceFilePath.toString().contains("ModelsClassified"))
						resultstream = fs.open(sourceFilePath);
					if(sourceFilePath.toString().contains("AccuracyInputData"))
						truthstream = fs.open(sourceFilePath);					
				}
				
					resultReader = new BufferedReader(new InputStreamReader(resultstream));
					truthReader = new BufferedReader(new InputStreamReader(truthstream));
					// Skip headers.
					resultReader.readLine(); truthReader.readLine();
					int count = 0, correct = 0;
					 int falsenegatives = 0;
	                    int falsepositives = 0;
					String resultLine, truthLine;
					while ((resultLine = resultReader.readLine()) != null && (truthLine = truthReader.readLine()) != null) {
						// Remove whitespace & parse.
						String[] resultFields = resultLine.replaceAll("\\s","").split(",");
						String[] truthFields = truthLine.replaceAll("\\s","").split(",");
						if (resultFields.length != 2 || truthFields.length != 2) {
							throw new Exception("Wrong number of fields; result:\n" + resultLine + "\nor truth:\n" + truthLine);
						}
						if (!resultFields[0].equals(truthFields[0])) {
							throw new Exception("Event Id mismatch; result: " + resultFields[0] + " vs. truth: " + truthFields[0]);
						}
						if (resultFields[1].equals(truthFields[1])) {
							correct++;
						}
						
                             
                        count++;
					}
					double result = (double)correct / (double)count;
					ctx.write(new Text("Accuracy"), new DoubleWritable(result));
					
					System.out.format("Accuracy= %.6f",result );
					
			}
				
	} catch (Exception e) {
					System.err.println(e.getMessage());
				}
				finally {
					try {if (resultReader != null) resultReader.close();} catch (IOException e) {}
					try {if (truthReader != null) truthReader.close();} catch (IOException e) {}
		}
				
			
	}
}
