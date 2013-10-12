package org.redneckdev.hadoop;

import org.apache.log4j.Logger;

//import java.io.IOException;
//import java.util.StringTokenizer;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.util.GenericOptionsParser;

/**
 * First stab at a Hadoop map-reduce app. The omnipresent word counter example, of course.
 * 
 * @author Robb
 * 
 */
public class BasicDriver {

	private final static Logger log = Logger.getLogger(BasicDriver.class);
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		log.info("BasicDriver starting up...");
		
		log.info("...BasicDriver finished");
	}

}
