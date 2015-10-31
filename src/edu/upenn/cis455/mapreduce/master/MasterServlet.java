package edu.upenn.cis455.mapreduce.master;

import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


/**
 * MasterServlet acts as a master in the MapReduce framework
 * master provides interface to client to request mapreduce work
 * and works as a coordinator to assign map and reduce jobs to workers
 * 
 * @author cis455
 *
 */
public class MasterServlet extends HttpServlet {
	
	Map<String, WorkerInfo> workersInfo = new HashMap<String, WorkerInfo>();

	static final long serialVersionUID = 455555001;
	private CollectWorker collectWorker;
	private CollectWorker finalCollectWorker;
	private boolean flag = true;
	

	/**
	 * sendMapPost method sends requests to workers to assign map work
	 * @param worker url
	 * @param job class
	 * @param input directory
	 * @param number of threads
	 * @param information of all the workers
	 * @param number of workers
	 * @throws UnsupportedEncodingException
	 */
	public void sendMapPost(String url, String job, String inputdirectory, int mapnumber, String workersinfo, int workersize) throws UnsupportedEncodingException{
		String urlParameters;
		urlParameters = "job="+URLEncoder.encode(job, "UTF-8")+
				"&inputdirectory="+URLEncoder.encode(inputdirectory, "UTF-8")+
				"&numthreads="+mapnumber+
				"&numworkers="+workersize+workersinfo; 
		url = url + "/worker/runmap";
		Client client = new Client(url);
		client.executePOST(urlParameters);
		System.out.println("finish send map request to worker "+url);
	}
	
	/**
	 * sendReducePost method send requests to workers to assign the reduce job
	 * @param worker url
	 * @param job class
	 * @param output directory
	 * @param number of threads
	 * @throws UnsupportedEncodingException
	 */
	public void sendReducePost(String url, String job, String outdirectory, int reducenumber) throws UnsupportedEncodingException{
		String urlParameters;
		urlParameters = "job="+URLEncoder.encode(job, "UTF-8")+
				"&outputdirectory="+URLEncoder.encode(outdirectory, "UTF-8")+
				"&numthreads="+reducenumber;
		url = url + "/worker/runreduce";
		Client client = new Client(url);
		client.executePOST(urlParameters);
	}

	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {
		// get post request, client submit the form
		// change flag to false, client cannot see the form during mapreduce period
		flag = false;
		PrintWriter out = response.getWriter();
		String job = request.getParameter("job");
		String inputDirectory = request.getParameter("inputdirectory");
		String outputDirectory = request.getParameter("outputdirectory");
		int mapnumber = Integer.parseInt(request.getParameter("mapnumber"));
		int reducenumber = Integer.parseInt(request.getParameter("reducenumber"));
		out.println("receive");
		out.println(job+inputDirectory+outputDirectory+mapnumber+reducenumber);
		
		// first assign index to each worker
		StringBuilder sb = new StringBuilder();
		List<String> activeworkers = new ArrayList<String>();
		int i = 0;
		for(Map.Entry<String, WorkerInfo> worker : workersInfo.entrySet()){
			if (isActiveWorker(worker)) {
				activeworkers.add(worker.getKey());
				sb.append("&worker"+i+"="+worker.getKey());
				i++;
			}
		}
		String workersinfo = new String(sb);
		collectWorker = new CollectWorker(activeworkers.size());
		
		// send post to each worker
		for(String worker:activeworkers){
			sendMapPost("http://"+worker, job, inputDirectory, mapnumber, workersinfo, activeworkers.size());
		}
		
		// check whether all the workers finish the map work, and the status are waiting
		synchronized(collectWorker){
			try {
				System.out.println("finish send map post, release the lock");
				collectWorker.wait();
				System.out.println("end finish");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		// all the workers have finished mapping job, master starts to send reduce work
		for(String worker:activeworkers){
			sendReducePost("http://"+worker, job, outputDirectory, reducenumber);
		}
		
		// when all the workers finished reducing job, change flag to true, client can sumbit another map-reduce work
		finalCollectWorker = new CollectWorker(activeworkers.size());
		synchronized (finalCollectWorker) {
			try {
				System.out.println("finish send reduce post, release the lock");
				finalCollectWorker.wait();
				System.out.println("end finish");
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		flag = true;
		collectWorker = null;
		finalCollectWorker = null;
		synchronized (workersinfo) {
			workersInfo = new HashMap<String, WorkerInfo>();
		}
	}

	/* (non-Javadoc)
	 * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
	 */
	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws java.io.IOException {
		String url = request.getRequestURI();
		if (url.endsWith("workerstatus")) {
			String ip = request.getRemoteAddr();
			int port = Integer.parseInt(request.getParameter("port"));
			String status = request.getParameter("status");
			String job = request.getParameter("job");
			int keysRead = Integer.parseInt(request.getParameter("keysread"));
			int keysWritten = Integer.parseInt(request.getParameter("keyswritten"));
			String keyword = ip + ":" + port;
			synchronized (workersInfo) {
				workersInfo.put(keyword, new WorkerInfo(ip, port, status, job,
						keysRead, keysWritten));
			}
			// each time check the status of workers, to see whether it is waiting(finish mapping)

			if(collectWorker != null){
				synchronized(collectWorker){
					if(status.equals("waiting")){
						System.out.println("waiting.."+keyword);
						collectWorker.addWorkers(keyword);
						if(collectWorker.isFinished()){
							System.out.println("all is waiting");
							collectWorker.setFinished();
							collectWorker.notifyAll();
						}
					}
				}
			}
			if(finalCollectWorker != null){
				synchronized (finalCollectWorker) {
					if(status.equals("idle")){
						finalCollectWorker.addWorkers(keyword);
						if(finalCollectWorker.isFinished()){
							finalCollectWorker.setFinished();
							finalCollectWorker.notifyAll();
						}
					}
					
				}
			}
			
			PrintWriter out = response.getWriter();
			out.println("workerstatus");
		} else if (url.endsWith("status")) {
			PrintWriter out = response.getWriter();
			out.println("<html>");
			out.println("<head><style>");
			out.println("table, th, td {border: 1px solid black;border-collapse: collapse;}");
			out.println("th, td {padding: 5px;text-align: left;}");
			out.println("</style></head>");
			out.println("<body>");
			out.println("<table style=\"width:100%\">");
			out.println("<caption><h2>Worker Status</h2></caption>");
			out.println("<tr><th>IP:port</th><th>Status</th><th>Job</th><th>Keys Read</th><th>Keys Written</th></tr>");
			for (Map.Entry<String, WorkerInfo> worker : workersInfo.entrySet()) {
				WorkerInfo workerinfo = worker.getValue();
				if (isActiveWorker(worker)) {
					out.println("<tr>");
					out.println("<td>" + worker.getKey() + "</td>");
					out.println("<td>" + workerinfo.getStatus() + "</td>");
					out.println("<td>" + workerinfo.getJob() + "</td>");
					out.println("<td>" + workerinfo.getKeysRead() + "</td>");
					out.println("<td>" + workerinfo.getKeysWritten() + "</td>");
					out.println("</tr>");
				}
			}
			if(flag){
				out.println("</table>");
				out.println("<h2>Job Form</h2>");
				out.println("<form action=\"status\" method=\"post\">");
				out.println("Class Name of Job:<br>");
				out.println("<input type=\"text\" name=\"job\">");
				out.println("<br><br>");
				out.println("Input Directory:<br>");
				out.println("<input type=\"text\" name=\"inputdirectory\">");
				out.println("<br><br>");
				out.println("OutputDirectory:<br>");
				out.println("<input type=\"text\" name=\"outputdirectory\">");
				out.println("<br><br>");
				out.println("Numbers of Map Threads to Run on Worker:<br>");
				out.println("<input type=\"text\" name=\"mapnumber\">");
				out.println("<br><br>");
				out.println("Numbers of Reduce Threads to Run on Worker:<br>");
				out.println("<input type=\"text\" name=\"reducenumber\">");
				out.println("<br><br>");
				out.println("<input type=\"submit\" value=\"Submit\">");
				out.println("</body></html>");
			}
		}
	}
	
	/**
	 * isActiveWorker method determines whether a worker is still active
	 * if a worker doesn't report in 30 seconds, it is regarded as inactive
	 * @param worker
	 * @return boolean value whether worker is active
	 */
	public boolean isActiveWorker(Map.Entry<String, WorkerInfo> worker){
		WorkerInfo workerinfo = worker.getValue();
		long last_report = workerinfo.getReportTime();
		Calendar cal = Calendar.getInstance();
		long current_time = cal.getTime().getTime();
		if ((current_time - last_report) <= 30000) return true;
		return false;
	}
}


/**
 * CollectWorker class records how many workers finish map work
 * @author cis455
 *
 */
class CollectWorker{
	private HashSet<String> finishedworkers;
	private	boolean finished;
	private int numworkers;
	
	public CollectWorker(int numworkers){
	
		this.numworkers = numworkers;
		finished = false;
		finishedworkers = new HashSet<String>();
	}
    
	public synchronized void setFinished(){
		finished = true;
	}
	public synchronized boolean isFinished(){
		int size = finishedworkers.size();
		return size == numworkers;
	}
	public synchronized void addWorkers(String worker){
		if(!finishedworkers.contains(worker)) finishedworkers.add(worker);
	}
}

