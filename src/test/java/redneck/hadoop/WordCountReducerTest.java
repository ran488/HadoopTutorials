package redneck.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class WordCountReducerTest {

	private WordCountReducer reducer;

	@Before
	public void setUp() throws Exception {
		reducer = new WordCountReducer();
	}

	@After
	public void tearDown() throws Exception {
		reducer = null;
	}

	
	@Test
	public void testReducerClass_1_Set() throws IOException {
		testReducerClass_Generic(1);
	}

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
