package edu.upenn.cis455.mapreduce.master;

import java.util.Calendar;

/**
 * WorkerInfo class records the information of a worker
 * including ip address, port number of worker server
 * including status, jobclass, time that worker reports to master
 * @author cis455
 *
 */
public class WorkerInfo {
	private String ip;
	private int port;
	private String status;
	private String job;
	private int keysRead;
	private int keysWritten;
	private long last_report;
	
	public WorkerInfo(String ip, int port, String status, String job, int keysRead, int keysWritten){
		this.ip = ip;
		this.port = port;
		this.status = status;
		this.job = job;
		this.keysRead = keysRead;
		this.keysWritten = keysWritten;
		Calendar cal = Calendar.getInstance();
    	this.last_report = cal.getTime().getTime();
	}
	
	public long getReportTime(){
		return last_report;
	}
	
	public String getIP(){
		return ip;
	}
	
	public int getPort(){
		return port;
	}
	
	public String getStatus(){
		return status;
	}
	
	public String getJob(){
		return job;
	}
	
	public int getKeysRead(){
		return keysRead;
	}
	
	public int getKeysWritten(){
		return keysWritten;
	}
	

}
