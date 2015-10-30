package edu.upenn.cis455.mapreduce.worker;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * WordQueue contains <key, value> pairs which need to be processed
 * and send to output file
 * The main thread will group <key, value> pairs with the same key
 * and insert the <key, list of values> to the queue
 * The reduce worker thread will take <key, list of values> from queue
 * and process the pair
 * @author cis455
 *
 */
public class WordQueue {
	private LinkedList<WordValues> wordQueue;
	private boolean finishRead;
	private int linenumber = 0;
	private int linecount;
	private int numworkers;
	
	/**
	 * initialize queue and finishRead sign
	 */
	public WordQueue(){
		wordQueue = new LinkedList<WordValues>();
		finishRead = false;
	}
	
	public WordQueue(int numworkers){
		wordQueue = new LinkedList<WordValues>();
		finishRead = false;
		this.numworkers = numworkers;
	}
	
	public synchronized void setFinish(){
		finishRead = true;
		notifyAll();
	}
	
	public boolean isEmpty(){
		return wordQueue.isEmpty();
	}
	
	public boolean finishRead(){
		return finishRead;
	}
	
	public synchronized void insertWord(String key, String[] values){
		linecount++;
		WordValues pair = new WordValues(key, values);
		wordQueue.add(pair);
		notify();
	}
	
	/**
	 * @return a chunk size of <key, list of values>
	 */
	public synchronized List<String> getChunkWord(){
		int chunksize = (linecount-linenumber)/numworkers;
		System.out.println("chunksize "+linecount+" "+linenumber);
		List<String> chunkfile = new ArrayList<String>();
		for(int i=0;i<chunksize;i++){
			WordValues wvs = getWord();
			if(wvs == null) break;
			String key = wvs.getKey();
			String[] values = wvs.getValues();
			chunkfile.add("key: "+key);
			for(String value:values){
				chunkfile.add("value: "+value);
			}
		}
		return chunkfile;
	}
	
	/**
	 * @return <key, list of values>
	 */
	public synchronized WordValues getWord(){
		while(isEmpty()){
			try {
				if(finishRead == true) return null;
				wait();
				if(finishRead == true) return null;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		WordValues pair = wordQueue.pop();
		linenumber++;
		return pair;
	}

}

class WordValues{
	String key;
	String[] values;
	
	public WordValues(String key, List<String> values){
		this.key = key;
		this.values = values.toArray(new String[values.size()]);
	}
	
	public WordValues(String key, String[] values){
		this.key = key;
		this.values = values;
	}
	
	public String getKey(){
		return key;
	}
	
	public String[] getValues(){
		return values;
	}
	
	public String toString(){
		StringBuilder sb = new StringBuilder();
		for(String value:values){
			sb.append(value+", ");
		}
		return key+": "+new String(sb);
	}
}
