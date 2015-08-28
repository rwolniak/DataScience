package com.boozallen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CharCount {
	
	/* Mapper which converts a line of text to individual characters and outputs each
	 * character along with a one (this value is used in summing the number of character
	 * occurrences)
	 */
	public static class TokenizerMapper 
	extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text aChar = new Text();

		public void map(Object key, Text value, Context context
                 ) throws IOException, InterruptedException {
			//split line up by character
			String [] chars = value.toString().split("");
			//write each character
			for(String str : chars){
				aChar.set(str);
				context.write(aChar, one);
			}
		}
	}

	//reducer which sums the number of occurrences of each character
	public static class IntSumReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
			int sum = 0;
			//loop through the list of values for this key, adding 1 to sum for each member of the list
			for (IntWritable val : values) {
				sum += val.get();
			}
			/* Set the sum and write to output, which is in the form 'key\tvalue'
			 * For example, 'c\t100', which is the character c, a tab, and the value 100
			 */
			result.set(sum);
			context.write(key, result);
		}
	}

}
