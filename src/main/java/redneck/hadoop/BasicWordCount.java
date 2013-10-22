package redneck.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.log4j.Logger;

/**
 * First stab at a Hadoop map-reduce app. The omnipresent word counter example,
 * of course.
 * 
 * @author Robb
 * 
 */
public class BasicWordCount {

	private final static Logger log = Logger.getLogger(BasicWordCount.class);
	JobConf conf;
	final static String PARAMETERS_ERR_MSG = "Required parameters: input_path output_path"; 
	
	
	/**
	 * Constructor: create a new job config and get it ready to go
	 * 
	 * @param inPath
	 * @param outPath
	 */
	public BasicWordCount(String inPath, String outPath) {
		conf = new JobConf(BasicWordCount.class);
		/*
		 * TODO I remember the book saying to use setJarByClass, but Apache
		 * example shows above. Need to go back and read that to remember why
		 * one is favorable over the other
		 * 
		 * conf.setJarByClass(BasicWordCount.class);
		 */
		conf.setJobName("basicWordCount");
		// TODO - lots of neat looking options to set on a JobConf - check those
		// out...
		conf.setJobPriority(JobPriority.NORMAL);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(WordCountMapper.class);
		conf.setCombinerClass(WordCountReducer.class);
		conf.setReducerClass(WordCountReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(inPath));
		FileOutputFormat.setOutputPath(conf, new Path(outPath));
	}

	/**
	 * Actually run the job.
	 * 
	 */
	public void run() throws IOException {
			JobClient.runJob(conf);
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws Exception {
		log.info("BasicWordCount starting up...");
		if (args.length < 2) {
			throw new Exception(PARAMETERS_ERR_MSG);
		} else {
			new BasicWordCount(args[0], args[1]).run();
		}
		log.info("...BasicWordCount finished");
	}

}