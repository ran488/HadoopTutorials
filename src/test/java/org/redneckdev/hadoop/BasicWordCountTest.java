/**
 * 
 */
package org.redneckdev.hadoop;

import static org.junit.Assert.*;

import java.io.IOException;

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

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		driver = new BasicWordCount();
		mapper = new BasicWordCount.WordCountMapper();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		driver = null;
		mapper = null;
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

}
