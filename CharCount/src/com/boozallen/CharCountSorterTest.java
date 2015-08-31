package com.boozallen;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class CharCountSorterTest {
	//drivers
	MapDriver<Object, Text, IntWritable, Text> mapDriver;
	ReduceDriver<IntWritable, Text, Text, IntWritable> reduceDriver;
	MapReduceDriver<Object, Text, IntWritable, Text, Text, IntWritable> mapReduceDriver;
	
	//set up drivers
	@SuppressWarnings("unchecked")
	@Before
	  public void setUp() {
	    CharCountSorter.SimpleMapper mapper = new CharCountSorter.SimpleMapper();
	    CharCountSorter.SimpleReducer reducer = new CharCountSorter.SimpleReducer();
	    CharCountSorter.IntComparator intcomp = new CharCountSorter.IntComparator();
	    mapDriver = MapDriver.newMapDriver(mapper);
	    reduceDriver = ReduceDriver.newReduceDriver(reducer);
	    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
	    //is not working. keys are being output in asc order
	    mapReduceDriver.setKeyOrderComparator(intcomp);
	  }
	 
	//test that a simple map works
	  @Test
	  public void testMapper() throws IOException {
	    mapDriver.withInput(new LongWritable(0L), new Text(
	        "T\t100"));
	    mapDriver.withOutput(new IntWritable(100), new Text("T"));
	    mapDriver.runTest();
	  }
	 
	  //test that a simple reduce works
	  @Test
	  public void testReducer() throws IOException {
	    List<Text> values = new ArrayList<Text>();
	    values.add(new Text("T"));
	    values.add(new Text("o"));
	    reduceDriver.withInput(new IntWritable(100), values);
	    reduceDriver.withOutput(new Text("T"), new IntWritable(100));
	    reduceDriver.withOutput(new Text("o"), new IntWritable(100));
	    reduceDriver.runTest();
	  }
	   
	  @Test
	  public void testMapReduce() throws IOException {
		    mapReduceDriver.withInput(new LongWritable(2L), new Text(
		              "-\t20"));
	    mapReduceDriver.withInput(new LongWritable(0L), new Text(
	              "T\t100"));
	    mapReduceDriver.withInput(new LongWritable(1L), new Text(
	              "o\t100"));
	    mapReduceDriver.withOutput(new Text("T"), new IntWritable(100));
	    mapReduceDriver.withOutput(new Text("o"), new IntWritable(100));
	    mapReduceDriver.withOutput(new Text("-"), new IntWritable(20));
	    //order matters in the output. We want descending order
	    mapReduceDriver.runTest(true);
	  }

}
