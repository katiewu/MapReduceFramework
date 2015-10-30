package edu.upenn.cis455.mapreduce.worker;

import java.io.IOException;
import java.util.List;

import edu.upenn.cis455.mapreduce.ContextMap;
import edu.upenn.cis455.mapreduce.Job;

/**
 * MapWorker class accomplishes the map work
 * get line from the input files and invoke map function
 * @author cis455
 *
 */
public class MapWorker implements Runnable {

	String jobclass;
	FileReadManager fm;
	String inputDirectory;
	String storageDir;
	ContextMap context;

	WordStatus wordStatus;

	/**
	 * @param job class
	 * @param fileReadManager
	 * @param storage directory
	 * @param wordStatus
	 * @param context
	 */
	public MapWorker(String jobclass, FileReadManager fm, String storageDir, WordStatus wordStatus, ContextMap context) {
		this.jobclass = jobclass;
		this.fm = fm;
		this.storageDir = storageDir;
		this.wordStatus = wordStatus;
		this.context = context;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run(){
		Class jobClass;
		try {
			jobClass = Class.forName(jobclass);
			Job job = (Job) jobClass.newInstance();
			String line;
			while((line = fm.readLine())!=null){
				String[] pair = line.split("\t", 2);
				if(pair.length == 2){
					wordStatus.plusWordsRead();
					String key = pair[0];
					String value = pair[1];
					job.map(key, value, context);
				}
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}


}
