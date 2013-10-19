package org.redneckdev.hadoop;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

/**
 * Reducer class to consolidate the multiple key/count pairs for a key into
 * one single pair for each key.
 */
public class WordCountReducer extends MapReduceBase implements
		Reducer<Text, IntWritable, Text, IntWritable> {

	private final static Logger log = Logger.getLogger(WordCountReducer.class);

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