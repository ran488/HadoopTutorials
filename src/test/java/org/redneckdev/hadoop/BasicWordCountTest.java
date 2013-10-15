/**
 * 
 */
package org.redneckdev.hadoop;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.easymock.*;

/**
 * @author Robb
 * 
 */
public class BasicWordCountTest {

	private BasicWordCount driver;
	private BasicWordCount.WordCountMapper mapper;
	private BasicWordCount.WordCountReducer reducer;;

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		driver = new BasicWordCount();
		mapper = new BasicWordCount.WordCountMapper();
		reducer = new BasicWordCount.WordCountReducer();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		driver = null;
		mapper = null;
		reducer = null;
	}

	/**
	 * TODO - make this more meaningful or remove it once all the individual
	 * pieces are tested.
	 */
	@Test
	public void testStaticMain() {
		BasicWordCount.main(new String[0]);
	}

	/**
	 * create mocks output and reporter so I can run this outside of Hadoop
	 * 
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testMapperClass_OneWord() throws IOException {
		LongWritable keyMock = new LongWritable();
		Text value = new Text("Test");
		OutputCollector<Text, IntWritable> outputMock = EasyMock
				.createMock(OutputCollector.class);
		Reporter reporterMock = EasyMock.createMock(Reporter.class);

		// expect my mocks to do this, then replay
		outputMock.collect(value, new IntWritable(1));
		EasyMock.replay(outputMock);
		// actual test method
		mapper.map(keyMock, value, outputMock, reporterMock);
		// did my mock do what I expected?
		EasyMock.verify(outputMock);
	}

	/**
	 * create mocks output and reporter so I can run this outside of Hadoop
	 * 
	 * @throws IOException
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testMapperClass_MultipleWords() throws IOException {
		LongWritable key = new LongWritable();
		Text value = new Text("test test test Test");
		OutputCollector<Text, IntWritable> output = EasyMock
				.createMock(OutputCollector.class);
		Reporter reporter = EasyMock.createMock(Reporter.class);

		output.collect(new Text("test"), new IntWritable(1));
		EasyMock.expectLastCall().times(3);
		output.collect(new Text("Test"), new IntWritable(1));
		EasyMock.replay(output);

		mapper.map(key, value, output, reporter);
		EasyMock.verify(output);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testReducerClass_1_Set() throws IOException {
		testReducerClass_Generic(1);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testReducerClass_100_Sets() throws IOException {
		testReducerClass_Generic(100);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testReducerClass_Non_1_Values() throws IOException {
		Text key = new Text("junit");
		List<IntWritable> numberValues = new ArrayList<IntWritable>();
		int total = 0;
		for (int ii = 0; ii < 10; ii++) {
			numberValues.add(new IntWritable(ii));
			total += ii;
		}
		Iterator<IntWritable> values = numberValues.iterator();
		OutputCollector<Text, IntWritable> output = EasyMock
				.createMock(OutputCollector.class);
		Reporter reporter = EasyMock.createMock(Reporter.class);
		output.collect(key, new IntWritable(total));
		EasyMock.replay(output);
		reducer.reduce(key, values, output, reporter);
		EasyMock.verify(output);
	}

	@SuppressWarnings("unchecked")
	private void testReducerClass_Generic(int numberOfSets) throws IOException {
		Text key = new Text("junit");
		Iterator<IntWritable> values = getReducerValues(numberOfSets);
		OutputCollector<Text, IntWritable> output = EasyMock
				.createMock(OutputCollector.class);
		Reporter reporter = EasyMock.createMock(Reporter.class);
		output.collect(key, new IntWritable(numberOfSets));
		EasyMock.replay(output);
		reducer.reduce(key, values, output, reporter);
		EasyMock.verify(output);
	}

	/** Create an Iterator to pass in to reducer */
	private Iterator<IntWritable> getReducerValues(int numberOfValues) {
		List<IntWritable> numberValues = new ArrayList<IntWritable>();
		for (int ii = 0; ii < numberOfValues; ii++) {
			numberValues.add(new IntWritable(1));
		}
		return numberValues.iterator();
	}

}
