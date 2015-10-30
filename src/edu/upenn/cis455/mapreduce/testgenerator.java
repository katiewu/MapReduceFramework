package edu.upenn.cis455.mapreduce;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class testgenerator {
	
	public static String[] generateRandomWords(int numberOfWords) {
		String[] randomStrings = new String[numberOfWords];
		Random random = new Random();
		for (int i = 0; i < numberOfWords; i++) {
			char[] word = new char[random.nextInt(1) + 3]; // words of length 3
															// through 10. (1
															// and 2 letter
															// words are
															// boring.)
			for (int j = 0; j < word.length; j++) {
				word[j] = (char) ('a' + random.nextInt(26));
			}
			randomStrings[i] = new String(word);
		}
		return randomStrings;
	}

	public static void main(String... args) throws IOException {
		for (int i = 1; i < 50; i++) {
			File f = new File("/home/cis455/store/test/input"
					+ i + ".txt");
			FileWriter fw = new FileWriter(f, true);
			for (int j = 0; j < 1000; j++) {
				for (String word : generateRandomWords(20)) {
					fw.write(word);
					fw.write("\t");
				}
				fw.write("\r");
			}
			fw.close();
		}
	}
	
	
}
