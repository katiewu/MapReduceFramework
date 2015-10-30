package edu.upenn.cis455.mapreduce;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;

import edu.upenn.cis455.mapreduce.worker.WordStatus;

/**
 * ContextMap provides functionality of writing lines to files for map workers
 * @author cis455
 *
 */
public class ContextMap implements Context{
	int numworkers;
	HashMap<Integer, BigInteger> ranges;
	String storageDir;
	WordStatus wordStatus;
	BufferedWriter[] bws;
	
	public ContextMap(int numworkers, String storageDir, WordStatus wordStatus) {
		this.numworkers = numworkers;
		this.storageDir = storageDir;
		this.wordStatus = wordStatus;
		generateRanges(numworkers);
		generateBufferWriter(numworkers);
	}
	
	private void generateBufferWriter(int n){
		bws = new BufferedWriter[n];
		for(int i=0;i<n;i++){
			String outputFile = storageDir+"spool-out/worker"+i+".txt";
			try {
				bws[i] = new BufferedWriter(new FileWriter(outputFile, true));
			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	private void generateRanges(int n) {
		String min = "0000000000000000000000000000000000000000";
		String max = "ffffffffffffffffffffffffffffffffffffffff";
		BigInteger maxInt = new BigInteger(max, 16);
		BigInteger minInt = new BigInteger(min, 16);
		BigInteger num = new BigInteger(Integer.toString(numworkers));

		BigInteger range = maxInt.divide(num);

		ranges = new HashMap<Integer, BigInteger>();
		for (int i = 0; i < numworkers; i++) {
			ranges.put(i, minInt.add(range));
			minInt = minInt.add(range);
		}
	}

	private static String getHash(String key) throws NoSuchAlgorithmException {
		MessageDigest md;
		md = MessageDigest.getInstance("SHA-1");
		md.update(key.getBytes());
		byte byteData[] = md.digest();
		StringBuffer sb = new StringBuffer();
		for (int j = 0; j < byteData.length; j++) {
			sb.append(Integer.toString((byteData[j] & 0xff) + 0x100, 16)
					.substring(1));
		}
		return sb.toString();
	}

	private int determineWorker(String key) {
		String hash;
		try {
			hash = getHash(key);
			BigInteger hashInt = new BigInteger(hash, 16);
			for (int i = 0; i < numworkers; i++) {
				if ((hashInt.compareTo(ranges.get(i))) < 0) {
					// System.out.println(key+" is assigned to worker"+i);
					return i;
				}
			}
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			return 0;
		}
		return 0;
	}

	@Override
	public void write(String key, String value) {
		wordStatus.plusWordsWritten();
		int index = determineWorker(key);
		try {
			synchronized (bws[index]) {
				bws[index].append(key+"\t"+value+"\r\n");
			}	
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void closeBufferWriter(){
		for(BufferedWriter bw:bws){
			try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
