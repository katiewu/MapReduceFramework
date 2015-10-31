package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import edu.upenn.cis455.mapreduce.ContextMap;
import edu.upenn.cis455.mapreduce.ContextReduce;

/**
 * WorkerServlet acts as a worker in MapReduce Framework
 * worker sends report to master
 * worker takes requests from master to do map and reduce work
 * worker steal works from other workers
 * @author cis455
 *
 */
public class WorkerServlet extends HttpServlet {

	static final long serialVersionUID = 455555002;
	private WordStatus wordStatus;
	private WorkerStatusSign status;
	
	private String portnumber;
	private String master;
	private String storageDir;
	
	private FileReadManager fm;
	private String[] workers;
	private int index = -1;
	private WordQueue queue;
	
	private ContextMap contextMap;
	private ContextReduce contextReduce;
	
	public void init(ServletConfig config) throws ServletException{
		master = config.getInitParameter("master");
		portnumber = config.getInitParameter("port");
		storageDir = config.getInitParameter("storageDir");
		status = new WorkerStatusSign(1);
		status.setStatus(Status.IDLE);
		wordStatus = new WordStatus();
		// a report thread, every 10 seconds send workerstatus to master
		Thread reportThread = new Thread(new ReportRunner(master, portnumber, status, wordStatus));
		reportThread.start();
	}
	
	private boolean deleteDir(File directory) {
		if (directory.exists()) {
			File[] files = directory.listFiles();
			if (null != files) {
				for (int i = 0; i < files.length; i++) {
					if (files[i].isDirectory()) {
						deleteDir(files[i]);
					} else {
						files[i].delete();
					}
				}
			}
		}
		return (directory.delete());
	}

	private void createSpool(int numworkers) {
		String spool_in = storageDir + "/spool-in";
		String spool_out = storageDir + "/spool-out";
		// create spool-in, spool-out folder
		createDir(spool_in);
		createDir(spool_out);
		// create files for each worker
		for (int i = 0; i < numworkers; i++) {
			String out_file = spool_out + "/worker" + i + ".txt";
			try {
				new File(out_file).createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}
	
	private void createOutput(String outputDir){
		createDir(outputDir);
		String output = outputDir + "/output.txt";
		try {
			new File(output).createNewFile();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void createDir(String dir){
		File directory = new File(dir);
		if(directory.exists()){
			deleteDir(directory);
		}
		directory.mkdir();
	}

	public void handlerunmap(HttpServletRequest request, HttpServletResponse response){
		// get map request data
		// clear previous work data
		fm = null;
		queue = null;
		workers = null;
		index = -1;
		contextMap = null;
		contextReduce = null;
		
		String jobclass = request.getParameter("job");
		String inputdirectory = request.getParameter("inputdirectory");
		if (inputdirectory.startsWith("/")) inputdirectory = storageDir + "/store"+inputdirectory;
		else inputdirectory = storageDir + "/"+inputdirectory;
		int numthreads = Integer.parseInt(request.getParameter("numthreads"));
		int numworkers = Integer.parseInt(request.getParameter("numworkers"));
		PrintWriter out;
		try {
			out = response.getWriter();
			out.println("receive map work");
			out.close();
			System.out.println("worker close the output");
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		
		status.setNumWorkers(numworkers);
		System.out.println("number of workers "+numworkers);
		// index worker
		workers = new String[numworkers];
		String localIP = request.getLocalAddr();
		int localPortnumber = request.getLocalPort();
		String key = localIP+":"+localPortnumber;
		
		for (int i = 0; i < numworkers; i++) {
			workers[i] = request.getParameter("worker"+i);
			if(workers[i].equals(key)) index = i;
		}

		// check local directories, spool-out, spool-in, create files
		createSpool(numworkers);
		System.out.println("finish create files");
		
		// instantiate multiple threads, read input and do the map work
		List<Thread> threads = new ArrayList<Thread>();
		// use a filereader to manage the bufferedReader of all the files that requires to process
		fm = new FileReadManager(inputdirectory, numworkers);
		contextMap = new ContextMap(numworkers, storageDir, wordStatus);
		status.setStatus(Status.MAPPING);
		status.setJob(jobclass);
		for (int i = 0; i < numthreads; i++) {
			Thread thread = new Thread(new MapWorker(jobclass, fm, storageDir, wordStatus, contextMap));
			threads.add(thread);
			thread.start();
		}	
		
		// wait all the threads to finish their map work
		for(Thread thread:threads){
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("worker finished his own job, start to steal work from others");
		
		// worker has already finished his own mapping job
		// worker starts to steal job from others
		HashSet<String> mappingSet = new HashSet<String>();
		for(int i=0;i<workers.length;i++){
			if(i != index) mappingSet.add(workers[i]);
		}
		
		while(!mappingSet.isEmpty()){
			List<String> remove = new ArrayList<String>();
			for(String worker:mappingSet){
				System.out.println("try steal "+worker);
				BufferedReader br = sendSteal(worker);
				if(br == null) remove.add(worker);
				else {
					processStealWork(br, numthreads, numworkers, jobclass);
				}
			}
			for(String worker:remove){
				mappingSet.remove(worker);
			}
		}
		System.out.println("worker has no work to do, finish mapping");
		contextMap.closeBufferWriter();
//		wordStatus.clearWordStatus();
		// threads end, just one main thread
		
		// main thread: partition the jobs to each worker, populate spool-in file /pushdata	
		for(int i=0;i<numworkers;i++){
			if(i != index){
				// send workeri.txt to worker i
				System.out.println("worker "+index+" push data to worker "+i);
				pushdata("http://"+workers[i]+"/worker/pushdata", i);
			}
			else{
				movedata(i);
			}
		}
		status.setJob(null);
		status.setStatus(Status.WAITING);

	}
	
	public void processStealWork(BufferedReader br, int numthreads, int numworkers, String jobclass){
		List<Thread> threads = new ArrayList<Thread>();
		// use a filereader to manage the bufferedReader of all the files that requires to process
		FileReadManager stealfm = new FileReadManager(br, numworkers);
		for (int i = 0; i < numthreads; i++) {
			Thread thread = new Thread(new MapWorker(jobclass, stealfm, storageDir, wordStatus, contextMap));
			threads.add(thread);
			thread.start();
		}	
		for(Thread thread:threads){
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	public void processReduceSteal(BufferedReader br, String jobclass, int numthreads){
		WordQueue stealqueue = new WordQueue();
		List<Thread> threads = new ArrayList<Thread>();
		for (int i = 0; i < numthreads; i++) {
			Thread thread = new Thread(new ReduceWorker(jobclass, stealqueue, contextReduce, wordStatus));
			threads.add(thread);
			thread.start();
		}
		String line;
		String key = null;
		List<String> values = null;
		try {
			while((line = br.readLine())!=null){
				System.out.println("process reduce steal: "+line);
				if(line.startsWith("key: ")){
					if(key!=null){
						stealqueue.insertWord(key, values.toArray(new String[values.size()]));
					}
					key = line.substring(5);
					values = new ArrayList<String>();
				}
				else{
					values.add(line.substring(7));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public BufferedReader sendSteal(String worker){
		String url = "http://"+worker+"/worker/steal";
		Client client = new Client(url);
		InputStream inputStream = client.executePost("");
		BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
	    	String line;
		try {
			line = in.readLine();
			System.out.println("send steal "+line);
			if(line.equals("worker hasn't yet finished")) return in;
			else return null;
		} catch (IOException e) {
			e.printStackTrace();
			return null;	
		}
	    
	}
	
	public BufferedReader sendReduceSteal(String worker){
		String url = "http://"+worker+"/worker/stealreduce";
		Client client = new Client(url);
		InputStream inputStream = client.executePost("");
		BufferedReader in = new BufferedReader(new InputStreamReader(inputStream));
		String line;
		try{
			line = in.readLine();
			System.out.println("try send reduce steal "+line);
			if(line.equals("worker has already finished")) return null;
			else return in;
		} catch(IOException e){
			e.printStackTrace();
			return null;
		}
	}
	
	private void pushdata(String url, int i){
		String storepath = storageDir+"spool-out/";
		storepath = storepath+"worker"+i+".txt";
		System.out.println("worker "+index+" send pushdata request, storepath: "+storepath);
		String body;
		try {
			body = new String(Files.readAllBytes(Paths.get(storepath)), StandardCharsets.UTF_8);
		} catch (IOException e) {
			body = "";
			e.printStackTrace();
		}
		Client client = new Client(url);
		client.executePOST(body);
		
	}
	
	private void movedata(int i){
		String storepath = storageDir+"spool-out/";
		storepath = storepath+"worker"+i+".txt";
		String newstorepath = storageDir+"spool-in/";
		// use current time to assign name
		Calendar cal = Calendar.getInstance();
    		long currenttime = cal.getTime().getTime();
   		newstorepath = newstorepath+currenttime+".txt";
   		System.out.println("worker "+index+" move data, original path: "+storepath+", new path: "+newstorepath);
		File file =new File(storepath);
		file.renameTo(new File(newstorepath));
		status.plusSpoolIn();
	}
	
	public void handlepushdata(HttpServletRequest request, HttpServletResponse response){
		System.out.println("worker "+index+" receive pushdata request");
		try {
			InputStream body = request.getInputStream();
			BufferedReader reader = new BufferedReader(new InputStreamReader(body));
			String line;
			Calendar cal = Calendar.getInstance();
	    		long currenttime = cal.getTime().getTime();
			String storepath = storageDir+"spool-in/"+currenttime+".txt";
			System.out.println("push data file "+storepath);
			File file = new File(storepath);
			file.createNewFile();
			System.out.println("create a new file");
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(storepath, true)));
			while ((line = reader.readLine()) != null) {
				out.println(line);
	        	}
			out.close();
			status.plusSpoolIn();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public void handlerunreduce(HttpServletRequest request, HttpServletResponse response) throws IOException{
		
		String jobclass = request.getParameter("job");
		String outputdirectory = request.getParameter("outputdirectory");
		int numthreads = Integer.parseInt(request.getParameter("numthreads"));
		try {
			PrintWriter out;
			out = response.getWriter();
			out.println("receive reduce work");
			out.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		outputdirectory = storageDir+ outputdirectory;
		System.out.println("outputdirectory "+outputdirectory);
		createOutput(outputdirectory);
		status.setStatus(Status.REDUCING);
		status.setJob(jobclass);
		wordStatus.clearWordStatus();
		
		// sort the file
		// concatenate all the files in spool-in directory into one file
		String spool_in_path = storageDir+"/spool-in";
		String catcommand = "cat "+spool_in_path+"/*.txt";
		String catoutfile = spool_in_path+"/cat.txt";
		CommandCreateFile(catcommand, catoutfile);
		System.out.println("finish cat");
		// sort file into sort.txt
		String sortcommand = "sort "+spool_in_path+"/cat.txt";
		String sortoutfile = spool_in_path+"/sort.txt";
		CommandCreateFile(sortcommand, sortoutfile);
		System.out.println("finish sort");
		
		
		queue = new WordQueue(workers.length);
		BufferedWriter output = new BufferedWriter(new FileWriter(outputdirectory+"/output.txt", true));
		contextReduce = new ContextReduce(wordStatus, output);
		// instantiate multiple threads, read input and do the map work
		// use a lock on the file that threads can write to the same file
		List<Thread> threads = new ArrayList<Thread>();
		for (int i = 0; i < numthreads; i++) {
			Thread thread = new Thread(new ReduceWorker(jobclass, queue, contextReduce, wordStatus));
			threads.add(thread);
			thread.start();
		}
		
		
		// main thread, read data from file and insert key, value pair to queue
		MasterInsertWord(sortoutfile, queue);
		
		// wait all the threads to finish their reduce work
		for(Thread thread:threads){
			try {
				thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		// worker finishes his own job, steal keys from other workers
		HashSet<String> reducingSet = new HashSet<String>();
		for(int i=0;i<workers.length;i++){
//			if(i != index) reducingSet.add(workers[i]);
			reducingSet.add(workers[i]);
		}
		
		while(!reducingSet.isEmpty()){
			List<String> remove = new ArrayList<String>();
			for(String worker:reducingSet){
				System.out.println("try steal "+worker);
				BufferedReader br = sendReduceSteal(worker);
				if(br == null) remove.add(worker);
				else processReduceSteal(br, jobclass, numthreads);
			}
			for(String worker:remove){
				reducingSet.remove(worker);
			}
		}	
		
		contextReduce.close();
		
		
		System.out.println("finish insert and threads finish get data");
		status.setStatus(Status.IDLE);
		status.setJob(null);
		wordStatus.clearWordsRead();
		
	}
	
	public void handlesteal(HttpServletRequest request, HttpServletResponse response) throws IOException{
		PrintWriter out = response.getWriter();
		System.out.println("handle steal fm is null? "+fm);
		if(fm == null){
			out.println("worker hasn't yet finished");
			out.close();
		}
		else{
			List<String> chunkfile = fm.readChunk();
			if(chunkfile.size() == 0){
				out.println("worker has already finished");
				out.close();
			}
			else{
				out.println("worker hasn't yet finished");
				for(String line:chunkfile){
					out.println(line);
				}
				out.close();
			}
		}
	}
	
	public void handlestealreduce(HttpServletRequest request, HttpServletResponse response){
		PrintWriter out;
		try {
			out = response.getWriter();
			if(queue == null){
				System.out.println("queue is null");
				out.println("worker hasn't yet finished");
				out.close();
			}
			else{
				System.out.println("get chunk word");
				List<String> body = queue.getChunkWord();
				if(body.size() == 0){
					out.println("worker has already finished");
					out.close();
				}
				else{
					out.println("worker hasn't yet finished");
					for(String line:body){
						out.println(line);
					}
					out.close();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	
	public void MasterInsertWord(String sortoutfile, WordQueue queue){	
		BufferedReader br = null;	 
		try {
			String line;		
			br = new BufferedReader(new FileReader(sortoutfile));
			String key = "";
			String newkey = "";
			List<String> values = new ArrayList<String>();
			while ((line = br.readLine()) != null) {
//				System.out.println("master thread: read line "+line);
				String[] pair = line.split("\t", 2);
				if(pair.length == 2){
					wordStatus.plusWordsRead();
//					System.out.println("master thread: split "+line);
					newkey = pair[0];
					if(key.equals("") || newkey.equals(key)){
						values.add(pair[1]);
						key = newkey;
					}
					else{
						// newkey is different from key, should insert previous key into queue
						String[] values_array = values.toArray(new String[values.size()]);
						queue.insertWord(key, values_array);
						key = newkey;
						values = new ArrayList<String>();
						values.add(pair[1]);
					}
				}
			}
			br.close();
			
			// at last time, also insert
			if(!key.equals("")){
				String[] values_array = values.toArray(new String[values.size()]);
				queue.insertWord(key, values_array);
			}
			// finish read, set finishRead to true
			queue.setFinish();
		} catch (IOException e) {
			e.printStackTrace();
		} 	
	}
	
	private static void CommandCreateFile(String command, String file) throws IOException{
		command = command+" > "+file;
		Process p = Runtime.getRuntime().exec(new String[]{"/bin/sh", "-c", command});
		try {
			p.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws IOException {
		String url = request.getRequestURI();
		portnumber = Integer.toString(request.getLocalPort());
		if (url.endsWith("runmap")) {
			handlerunmap(request, response);
		}
		else if(url.endsWith("pushdata")){
			handlepushdata(request, response);
		}
		else if(url.endsWith("runreduce")){
			handlerunreduce(request, response);
		}
		else if(url.endsWith("steal")){
			handlesteal(request, response);
		}
		else if(url.endsWith("stealreduce")){
			handlestealreduce(request, response);
		}
	}

	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws IOException {
		response.setContentType("text/html");
		PrintWriter out = response.getWriter();
		out.println("Hi, I am the worker!");
	}
}
