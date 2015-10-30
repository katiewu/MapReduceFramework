package edu.upenn.cis455.mapreduce.worker;

import java.util.List;

import edu.upenn.cis455.mapreduce.ContextReduce;
import edu.upenn.cis455.mapreduce.Job;

/**
 * ReduceWorker class accomplishes reduce job
 * read line from local disk and invoke reduce function
 * @author cis455
 *
 */
public class ReduceWorker implements Runnable{
	
	private String jobclass;
	private WordQueue queue;
	private ContextReduce context;
	private WordStatus wordStatus;
	
	/**
	 * @param job class
	 * @param queue
	 * @param context
	 * @param wordStatus
	 */
	public ReduceWorker(String jobclass, WordQueue queue, ContextReduce context, WordStatus wordStatus){
		this.jobclass = jobclass;
		this.queue = queue;
		this.context = context;
		this.wordStatus = wordStatus;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Runnable#run()
	 */
	@Override
	public void run() {
		while(!(queue.isEmpty() && queue.finishRead())){
			Class jobClass;
			try {
				jobClass = Class.forName(jobclass);
				Job job = (Job)jobClass.newInstance();
				WordValues pair = queue.getWord();
				// pair has already get the sign that queue is empty and master thread has finished reading
				if(pair == null) break;
				String key = pair.getKey();
				String[] values = pair.getValues();
				job.reduce(key, values, context);
				if(queue.isEmpty() && queue.finishRead()) break;
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InstantiationException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				e.printStackTrace();
			}
		}
		
		
		
	}

}
