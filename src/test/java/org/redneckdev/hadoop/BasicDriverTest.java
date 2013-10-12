/**
 * 
 */
package org.redneckdev.hadoop;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Robb
 *
 */
public class BasicDriverTest {

	private BasicDriver driver;
	
	/**
	 * @throws java.lang.Exception
	 */
	@Before
	public void setUp() throws Exception {
		driver = new BasicDriver();
	}

	/**
	 * @throws java.lang.Exception
	 */
	@After
	public void tearDown() throws Exception {
		driver = null;
	}

	/** 
	 * TODO - make this more meaningful or remove it once all the individual pieces are tested.
	 */
	@Test
	public void testStaticMain() {
		BasicDriver.main(new String[0]);
	}

}
