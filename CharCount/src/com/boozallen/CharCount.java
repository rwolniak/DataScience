package com.boozallen;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CharCount {
	
	
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
//			StringTokenizer itr = new StringTokenizer(value.toString());
//			while (itr.hasMoreTokens()) {
//				word.set(itr.nextToken());
//				context.write(word, one);
//			}
		}
	}

	public static class IntSumReducer
    extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
                    Context context
                    ) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

}
