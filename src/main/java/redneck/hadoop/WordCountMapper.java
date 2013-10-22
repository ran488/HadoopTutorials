package redneck.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.log4j.Logger;

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
public class WordCountMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, IntWritable> {

	final static Logger log = Logger.getLogger(WordCountMapper.class);
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