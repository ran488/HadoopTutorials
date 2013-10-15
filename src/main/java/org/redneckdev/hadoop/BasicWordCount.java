package org.redneckdev.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
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

	/**
	 * Mapper class for my word count example. Yes, this is stolen right from
	 * the Apache tutorial, but I'm stealing it line by line and adding logging
	 * and unit tests so I actually understand what's going on.
	 * 
	 * My unit test is pointing out some interesting stuff. A straight up string
	 * tokenizer probably won't get you the results you may expect. Shows the
	 * importance of unit tests in general, and more specifically for this case,
	 * understanding what your Hadoop mapper is doing, before you rely on the
	 * results of a hours-long run on a huge data set. Results may be skewed if
	 * you don't expect "test", "test.", and "test?" to be 3 different words.
	 */
	public static class WordCountMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable ONE = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String lineOfText = value.toString();
			if (log.isDebugEnabled())
				log.debug(String.format("Mapper working on input: [%s]",
						lineOfText));
			StringTokenizer tokenizer = new StringTokenizer(lineOfText);

			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, ONE);
				if (log.isTraceEnabled())
					log.trace(String.format(
							" + Mapper just collected a word: [%s]",
							word.toString()));
			}

		}
	}

	/**
	 * Reducer class to consolidate the multiple key/count pairs for a key into
	 * one single pair for each key.
	 */
	public static class WordCountReducer extends MapReduceBase implements
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			
			if (log.isDebugEnabled())
				log.debug(String.format("Reducer running for key [%s]",
						key.toString()));
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			
			if (log.isDebugEnabled())
				log.debug(String.format(
						"Reducer found [%d] instances of key [%s]", sum,
						key.toString()));
			output.collect(key, new IntWritable(sum));
		}
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		log.info("BasicDriver starting up...");

		log.info("...BasicDriver finished");
	}

}
