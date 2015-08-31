package com.boozallen;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CharCountSorter {
	
	//comparator used to compare integers
	public static class IntComparator extends WritableComparator {

	     public IntComparator() {
	         super(IntWritable.class);
	     }

	     @Override
	     public int compare(byte[] b1, int s1, int l1,
	             byte[] b2, int s2, int l2) {
	         Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
	         Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();
	         System.out.println(v1 + " " + v2 + " " + v1.compareTo(v2));
	         return v1.compareTo(v2) * -1;
	     }
	 }
	
	//mapper which takes in the output from CharCount and outputs it as val,key so that a secondary sort will occur
	public static class SimpleMapper 
	extends Mapper<Object, Text, IntWritable, Text>{
		private Text aChar = new Text();

		public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
			//split the string by tab
			String line = value.toString();
			String[] tokens = line.split("\t");
			
			//write the string if the token is an integer
			try {
				int valuePart = Integer.parseInt(tokens[1]);
				aChar.set(tokens[0]);
				context.write(new IntWritable(valuePart), aChar);
			} catch (NumberFormatException e){
				//catch the error if the token is not an integer
				System.err.println("ERROR :  token was not an integer : " + tokens[1]);
			}
			
		}
	}

	//output the sorted list of characters (sorted by number of occurrences)
	public static class SimpleReducer
    extends Reducer<IntWritable,Text,Text,IntWritable> {

		public void reduce(IntWritable key, Iterable<Text> values,
                    Context context
                    ) throws IOException, InterruptedException {
			for (Text val : values) {
				context.write(val, key);
			}
		}
	}
}
