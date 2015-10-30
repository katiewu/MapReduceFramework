package edu.upenn.cis455.mapreduce.worker;

public class WorkerStatusSign {

	private Status status;
	private String job;
	private int numworkers;
	private int spool_in_numbers;

	public WorkerStatusSign(int numworkers) {
		spool_in_numbers = 0;
		this.numworkers = numworkers;
	}
	
	public synchronized void setNumWorkers(int n){
		numworkers = n;
	}
	
	public synchronized void setJob(String s){
		job = s;
	}

	public synchronized void setStatus(Status s) {
		status = s;
	}
	
	public synchronized void plusSpoolIn(){
		spool_in_numbers++;
		if(spool_in_numbers == numworkers){
			setStatus(Status.WAITING);
		}
	}
	
	
	public String getJob(){
		return job;
	}

	public String getStatus() {
		switch (status) {
		case MAPPING:
			return "mapping";

		case REDUCING:
			return "reducing";

		case IDLE:
			return "idle";
			
		case WAITING:
			return "waiting";

		default:
			return null;
		}
	}

}

enum Status {
	MAPPING, REDUCING, IDLE, WAITING
}