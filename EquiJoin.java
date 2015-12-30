package mapreduce.equijoin;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Hello world!
 *
 */
public class EquiJoin 
{
	// Mapper Class
	public static class Join_Mapper extends MapReduceBase implements
		Mapper<Object, Text, Text, Text> {

		public void map(Object arg0, Text input, OutputCollector<Text, Text> output, Reporter arg3) throws IOException {
			String tuple = input.toString();
			String[] tupleValues = tuple.split(",");
			String key = tupleValues[1].trim();
			
			output.collect(new Text(key), new Text(tuple));
		}
	}
	
	// Reducer Class
	public static class Join_Reducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

		public void reduce(Text key, Iterator<Text> tupleValues, OutputCollector<Text, Text> output, Reporter arg3)
				throws IOException {

			String[] arrangedTuples = new String[100000];
			int i = 0;
			
			while(tupleValues.hasNext()) {
				arrangedTuples[i++] = tupleValues.next().toString();
			}
			
			for(int j = 0; j < i; j++) {
				String[] tuple = arrangedTuples[j].split(",");
			//	if(tuple[0].trim().equals("R")) {
					for(int k = j + 1 ; k < i; k++) {
						StringBuffer result = new StringBuffer();
						StringBuffer resultKey = new StringBuffer();
						String[] newTuple = arrangedTuples[k].split(",");
						if(!newTuple[0].trim().equals(tuple[0].trim())) {
							resultKey.append(arrangedTuples[j] + ',');
							result.append(arrangedTuples[k]);
							output.collect(new Text(resultKey.toString()), new Text(result.toString()));
						}
					}
				//}
			}
		}
	}
	
    public static void main(String[] args) throws Exception
    {
    	String jobName = "Equi-Join";
    	
    	JobConf conf = new JobConf(EquiJoin.class);
    	
		conf.setJobName(jobName);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Join_Mapper.class);
		conf.setReducerClass(Join_Reducer.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
    }
}
