package edu.upenn.cis455.mapreduce.worker;

/**
 * WordStatus keeps status of words read and words written
 * @author cis455
 *
 */
public class WordStatus {
	private int wordsRead;
	private int wordsWritten;
	
	public WordStatus(){
		wordsRead = 0;
		wordsWritten = 0;
	}
	
	/**
	 * increment words read by one
	 */
	public synchronized void plusWordsRead(){
		wordsRead++;
	}
	
	/**
	 * increment words written by one
	 */
	public synchronized void plusWordsWritten(){
		wordsWritten++;
	}
	
	/**
	 * clear the status of words read and words written
	 */
	public synchronized void clearWordStatus(){
		wordsRead = 0;
		wordsWritten = 0;
	}
	
	public synchronized void clearWordsRead(){
		wordsRead = 0;
	}
	
	/**
	 * @return words read
	 */
	public int getWordsRead(){
		return wordsRead;
	}
	
	/**
	 * @return words written
	 */
	public int getWordsWritten(){
		return wordsWritten;
	}
}
