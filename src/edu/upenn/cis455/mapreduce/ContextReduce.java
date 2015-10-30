package edu.upenn.cis455.mapreduce;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import edu.upenn.cis455.mapreduce.worker.WordStatus;

/**
 * ContextReduce provides reduce workers to write lines to output file
 * @author cis455
 *
 */
public class ContextReduce implements Context{
	
	private WordStatus wordStatus;
	private BufferedWriter output;
	
	public ContextReduce(WordStatus wordStatus, BufferedWriter output) throws IOException{
		this.wordStatus = wordStatus;
		this.output = output;
	}

	@Override
	// each time just one thread can write word to the file
	public synchronized void write(String key, String value) {
			try {
				output.append(key+"\t"+value+"\r\n");
				wordStatus.plusWordsWritten();
			} catch (IOException e) {
				e.printStackTrace();
			}	
	}
	
	public void close(){
		try {
			output.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

