/**
 * 
 */
package org.redneckdev.hadoop;

import static org.junit.Assert.*;

import java.io.File;
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
	private final static String inPath;
	private final static String outPath;

	static {
		File in = new File("build/tmp/junitInPath");
		File out = new File("build/tmp/junitOutPath");
		in.mkdir();
		out.mkdir();
		inPath = in.getAbsolutePath();
		outPath = out.getAbsolutePath();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		driver = new BasicWordCount(inPath, outPath);
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		driver = null;
	}

	@Test
	public void verifyConstructor() {
		assertNotNull(driver.conf);
		assertEquals(WordCountMapper.class, driver.conf.getMapperClass());
		assertEquals(WordCountReducer.class, driver.conf.getReducerClass());
	}

	/**
	 * TODO - make this more meaningful or remove it once all the individual
	 * pieces are tested.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testStaticMain_OneArgShouldFail() {
		String[] args = new String[1];
		args[0] = inPath;
		try {
			BasicWordCount.main(args);
			fail("Main should have failed with an exception, but did not.");
		} catch (Exception e) {
			if (!BasicWordCount.PARAMETERS_ERR_MSG.equals(e.getMessage())) {
				fail("Method threw an unexpected exception.");
				e.printStackTrace();
			}
		}
	}
}
