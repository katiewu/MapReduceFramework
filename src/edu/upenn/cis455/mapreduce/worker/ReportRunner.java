package edu.upenn.cis455.mapreduce.worker;


/**
 * ReportRunner send reports to master each 10 seconds
 * The report includes the status of worker, the number of keys read and keys written
 * @author cis455
 *
 */
public class ReportRunner implements Runnable{

	String master;
	String portnumber;
	WorkerStatusSign status;
	WordStatus wordStatus;
	
	/**
	 * @param IP address and port number of master
	 * @param Portnumber workers listen on
	 * @param worker status
	 * @param wordStatus
	 */
	public ReportRunner(String master, String portnumber, WorkerStatusSign status, WordStatus wordStatus){
		this.master = master;
		this.portnumber = portnumber;
		this.status = status;
		this.wordStatus = wordStatus;
	}
	
	@Override
	public void run() {
		
		while(true){
			String masterpath = "http://"+master+"/master/workerstatus";
			String query = "port="+portnumber+"&status="+status.getStatus()+"&job="+status.getJob()+"&keysread="+wordStatus.getWordsRead()+"&keyswritten="+wordStatus.getWordsWritten();
			Client client = new Client(masterpath);
			client.executeGET(query);
			try {
				Thread.sleep(10000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
	}

}
