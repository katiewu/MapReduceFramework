package edu.upenn.cis455.mapreduce.worker;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.List;

/**
 * FileReadManager acts as a manager to manage file reading
 * FileReadManager manages all the files in a folder
 * FileReadManager provides interface of reading line or reading
 * a chunk size of lines from files in a folder
 * The files in a folder seem like one file to clients
 * 
 * @author cis455
 *
 */
public class FileReadManager {

	private String inputdirectory;
	private File folder;
	private List<File> files = new ArrayList<File>();
	private int filesize;
	private BufferedReader br;
	private int index;
	
	private String inputbody;
	private int chunksize = 100;
	private int linenumber = 0;
	private int linecount = 0;
	private int numworkers;
	
	/**
	 * initialize FileReadManager by using BufferedReader
	 * @param br
	 * @param numworkers
	 */
	public FileReadManager(BufferedReader br, int numworkers){
		this.br = br;
		this.numworkers = numworkers;
		this.filesize = 1;
	}
	
	/**
	 * initialize FileReadManager by using a folder
	 * @param inputdirectory
	 * @param numworkers
	 */
	public FileReadManager(String inputdirectory, int numworkers){
		this.inputdirectory = inputdirectory;
		this.numworkers = numworkers;
		folder = new File(inputdirectory);
		File[] tempfiles = folder.listFiles();
		for(int i=0;i<tempfiles.length;i++){
			if (tempfiles[i].isFile() && !tempfiles[i].getName().endsWith("~")) {
				files.add(tempfiles[i]);
				linecount += LineCount(tempfiles[i]);
			}
		}
		filesize = files.size();
		index = 0;
		if(files.size() == 0) br = null;
		else{
			try {
				br = new BufferedReader(new FileReader(files.get(index).getAbsolutePath()));
			} catch (FileNotFoundException e) {
				e.printStackTrace();
				br = null;
			}	
		}
	}
	
	public int getLinecount(){
		return linecount;
	}
	
	/**
	 * get number of lines in a file
	 * @param file
	 * @return number of lines
	 */
	private int LineCount(File file){
		try {
			LineNumberReader  lnr = new LineNumberReader(new FileReader(file));
			lnr.skip(Long.MAX_VALUE);
			lnr.close();
			return lnr.getLineNumber();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return 0;
		
	}
	
	/**
	 * read a chunk of lines from files
	 * the chunk size is determined by the remaining of lines and
	 * the number of workers
	 * @return  a list of lines
	 * @throws IOException
	 */
	public synchronized List<String> readChunk() throws IOException{
		chunksize = (linecount-linenumber)/numworkers;
		List<String> chunkfile = new ArrayList<String>();
		for(int i=0;i<chunksize;i++){
			String line = readLine();
			if(line == null) return chunkfile;
			chunkfile.add(line);
		}
		return chunkfile;
	}
	
	/**
	 * read line from files
	 * @return line
	 * @throws IOException
	 */
	public synchronized String readLine() throws IOException{
		linenumber++;
		if(br == null) {
			return null;
		}
		String line = br.readLine();
		if(line != null){
			return line;
		}
		else{
			if(index == (filesize - 1)){
				// read all the files in the folder, return null
				return null;
			}
			else{
				index++;
				br = new BufferedReader(new FileReader(files.get(index).getAbsolutePath()));
				line = br.readLine();
				return line;
			}
		}
	}
	
	
}
