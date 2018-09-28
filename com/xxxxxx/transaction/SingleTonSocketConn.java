package com.xxxxxx.transaction;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;
import org.jpos.util.Log;



public class SingleTonSocketConn implements Runnable{
	private static Socket socketInstance;
	private static ServerSocket serverSocket;
	private static OutputStream outputStream;
	private static InputStream inputStream;
	boolean SERVER_RUNNING=true;

	public SingleTonSocketConn()
	{
		createServer();
	}

	public void run()
	{
		while (SERVER_RUNNING)
		{
			acceptClient();
			logger.info("Connected to Client "+socketInstance.getInetAddress()+" , "+socketInstance.getPort());
		}

	}

	private static void createServer()
	{
		try {
			int port=Integer.parseInt(Configuration.getValue(Configuration.SERVER_PORT));
			serverSocket=new ServerSocket(port);
			logger.info("Server Socket created at "+serverSocket.getInetAddress()+" PORT "+serverSocket.getLocalPort());

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	static Logger logger = Logger.getLogger("com.xxxxxx.logger");

	public static Socket getInstance(){
		return socketInstance;
	}

	public static void acceptClient() {

		try {
			socketInstance=serverSocket.accept();
			socketInstance.setKeepAlive(true);
			outputStream= socketInstance.getOutputStream();
			inputStream=socketInstance.getInputStream();

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public static synchronized OutputStream getOutputStream() {

		return outputStream;

	}
	public static synchronized InputStream getInputStream() {

		return inputStream;

	}

	public static void setNull() {
		socketInstance=null;
		
	}
}
