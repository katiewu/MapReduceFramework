package edu.upenn.cis455.mapreduce.job;

import java.util.StringTokenizer;

import edu.upenn.cis455.mapreduce.Context;
import edu.upenn.cis455.mapreduce.Job;

public class WordCount implements Job {

	public void map(String key, String value, Context context) {
		StringTokenizer tokenizer = new StringTokenizer(value);
		String word = "";
	        while (tokenizer.hasMoreTokens()) {
	            word = tokenizer.nextToken();
	            context.write(word, "1");
	        }
	}

	public void reduce(String key, String[] values, Context context) {
		int sum = values.length;
		context.write(key, Integer.toString(sum));
	}


}
