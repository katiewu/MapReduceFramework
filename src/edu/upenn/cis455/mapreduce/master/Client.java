package edu.upenn.cis455.mapreduce.master;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URL;
import java.net.URLEncoder;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.net.ssl.HttpsURLConnection;


/**
 * Client class accomplish the job of visiting a url
 * can send both get and post methods
 * 
 * @author cis455
 *
 */

public class Client {
	String url;
	String hostName;
	String path;
	int portNumber;
	int contentLength;
	String contentType = "text/html";
	long last_modified; 
	
	/**
	 * a client is associated with a url
	 * @param url
	 */
	public Client(String url){
		this.url = url;
		URLInfo urlinfo = new URLInfo(url);
		this.hostName = urlinfo.getHostName();
		this.path = urlinfo.getFilePath();
		this.portNumber = urlinfo.getPortNo();
	}
	
	/**
	 * send request using get method to a url
	 * @param query to be assigned to request
	 * @return response as inputstream
	 */
	public InputStream executeGET(String query){
		if(url.startsWith("https")){
			URL https_url;
			try {
				if(query.equals("")) https_url = new URL(url);
				else https_url = new URL(url+"?"+query);
				HttpsURLConnection urlConnection = (HttpsURLConnection)https_url.openConnection();
				urlConnection.connect();
				contentLength = urlConnection.getContentLength();
				contentType = urlConnection.getContentType();
				last_modified = urlConnection.getLastModified();
				return urlConnection.getInputStream();
			} catch (MalformedURLException e) {
				e.printStackTrace();
				return null;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
		else if(url.startsWith("http")){
//			System.out.println("http");
			Socket socket;
			try {
//				System.out.println(hostName+" "+portNumber);
				socket = new Socket(InetAddress.getByName(hostName), portNumber);
				//send HEAD request
				PrintWriter pw = new PrintWriter(socket.getOutputStream());
				if(query.equals("")) pw.println("GET "+path+" HTTP/1.0");
				else pw.println("GET "+path+"?"+query+" "+"HTTP/1.0");
				pw.println("Host: "+hostName);
				pw.println("User-Agent:cis455crawler");
				pw.println("");
				pw.flush();
				
				BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String s;
				while((s = br.readLine()) != null){
					if(s.equals("")) break;
				}
				StringBuilder sb = new StringBuilder();
				while((s = br.readLine()) != null){
					sb.append(s);
					sb.append("\n");
				}
//				char[] body = new char[contentLength];
//				br.read(body, 0, contentLength);
				String responseBody = new String(sb);
				return new ByteArrayInputStream(responseBody.getBytes());
			} catch (UnknownHostException e) {
				e.printStackTrace();
				return null;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			} 
		}
		return null;
	}
	
	/**
	 * send request by using post method to a url
	 * the method just sends the request, don't accept the response
	 * @param body associated with the request
	 */
	public void executePOST(String urlParameters){
		if(url.startsWith("https")){
			URL https_url;
			try {
				https_url = new URL(url);
				HttpsURLConnection urlConnection = (HttpsURLConnection)https_url.openConnection();
				urlConnection.setRequestMethod("POST");
				urlConnection.setRequestProperty("User-Agent:", "cis455crawler");
				urlConnection.setDoOutput(true);
				DataOutputStream wr = new DataOutputStream(urlConnection.getOutputStream());
				wr.writeBytes(urlParameters);
				wr.flush();
				wr.close();

				contentLength = urlConnection.getContentLength();
				contentType = urlConnection.getContentType();
				last_modified = urlConnection.getLastModified();
			} catch (MalformedURLException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else if(url.startsWith("http")){
			System.out.println("execute post: http");
			Socket socket;
			try {
				System.out.println(hostName+" "+portNumber);
				socket = new Socket(InetAddress.getByName(hostName), portNumber);
				//send HEAD request
				BufferedWriter pw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));
				pw.write("POST " + url + " HTTP/1.0\r\n");
				pw.write("Host: " + hostName+"\r\n");
				pw.write("User-Agent: cis455crawler\r\n");
				pw.write("Content-Length: " + urlParameters.length() + "\r\n");
				pw.write("Content-Type: application/x-www-form-urlencoded\r\n");
				pw.write("\r\n");
				pw.write(urlParameters);
			    pw.flush();
				
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
	}
	
	/**
	 * send request by using method post to a url
	 * the method sends the request, and also receive the response
	 * @param body
	 * @return response as inputstream
	 */
	public InputStream executePost(String urlParameters){
		if(url.startsWith("https")){
			URL https_url;
			try {
				https_url = new URL(url);
				HttpsURLConnection urlConnection = (HttpsURLConnection)https_url.openConnection();
				urlConnection.setRequestMethod("POST");
				urlConnection.setRequestProperty("User-Agent:", "cis455crawler");
				urlConnection.setDoOutput(true);
				DataOutputStream wr = new DataOutputStream(urlConnection.getOutputStream());
				wr.writeBytes(urlParameters);
				wr.flush();
				wr.close();

				contentLength = urlConnection.getContentLength();
				contentType = urlConnection.getContentType();
				last_modified = urlConnection.getLastModified();
				return urlConnection.getInputStream();
			} catch (MalformedURLException e) {
				e.printStackTrace();
				return null;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
		}
		else if(url.startsWith("http")){
			System.out.println("execute post: http");
			Socket socket;
			try {
				System.out.println(hostName+" "+portNumber);
				socket = new Socket(InetAddress.getByName(hostName), portNumber);
				//send HEAD request
				BufferedWriter pw = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), "UTF-8"));
				pw.write("POST " + url + " HTTP/1.0\r\n");
				pw.write("Host: " + hostName+"\r\n");
				pw.write("User-Agent: cis455crawler\r\n");
				pw.write("Content-Length: " + urlParameters.length() + "\r\n");
				pw.write("Content-Type: application/x-www-form-urlencoded\r\n");
				pw.write("\r\n");
				pw.write(urlParameters);
			    pw.flush();
				
				BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String s;
				while((s = br.readLine()) != null){
					if(s.equals("")) break;
				}
				StringBuilder sb = new StringBuilder();
				while((s = br.readLine()) != null){
					sb.append(s);
					sb.append("\n");
				}
//				char[] body = new char[contentLength];
//				br.read(body, 0, contentLength);
				String responseBody = new String(sb);
				pw.close();
				br.close();
				return new ByteArrayInputStream(responseBody.getBytes());
			} catch (UnknownHostException e) {
				e.printStackTrace();
				return null;
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			} 
		}
		return null;
	}
	
	/**
	 * process the initial line of the response to determine whether request is valid
	 * @param initial line
	 * @return whether request is valid
	 */
	public boolean processInitialLine(String s){
		Pattern p = Pattern.compile("HTTP/1.0 (\\d{3}) .*");
		Matcher m = p.matcher(s);
		if(m.find()){
			int status_code = Integer.parseInt(m.group(1));
			if(status_code<400) return true;
		}
		return false;
	}
	
	
		
}
