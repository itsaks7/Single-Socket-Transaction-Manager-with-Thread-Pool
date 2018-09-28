package com.xxxxxx.transaction;

import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import com.xxxxxx.logger.LogParser;
import com.xxxxxx.socketClasses.ReadResponse;


/**
 * Read InTable Send request to Acquirer Response timeout wait for 15 seconds if
 * failed Put in SAF Start reversal process Put in OUT
 */

public class TransactionManager {

	private static final long HSQL_READ_DELAY = 500;
	private static final int STALE_TRANSACTION_TIMEOUT = 20;
	private static ThreadPoolExecutor threadPool = null;
	private static final int THREAD_POOL_SIZE = 300;
	private static final int MAX_POOL_SIZE = 500;
	private static final long KEEP_ALIVE_TIME = 1;
	public static final int TRANSACTION_TYPE_LOG_ON_RESPONSE = 22;
	public static final int TRANSACTION_TYPE_ECHO_RESPONSE = 23;
	public static final int TRANSACTION_TYPE_LOG_OFF_RESPONSE = 24;
	public static final int TRANSACTION_TYPE_CUTOVER_REQUEST = 80;
	public static final boolean SERVER_RUNNING = true;
	public static Socket clientSocket;
	public static ServerSocket serverSocket;
	
	private static final ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(
			5000);
	static MYSQLConnection mySqlConnection = new MYSQLConnection();
	private static Logger logger = Logger.getLogger(TransactionManager.class);
	
	public static void main(String[] args) {
		try {
			DOMConfigurator.configure("log4j.xml");
			
			threadPool = new ThreadPoolExecutor(THREAD_POOL_SIZE,
					MAX_POOL_SIZE, KEEP_ALIVE_TIME, TimeUnit.SECONDS, queue);
			
			logger.log(Level.INFO, "Transaction Manager Started");
			ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
					
			scheduler.scheduleWithFixedDelay(new LogParser("logs/application.log", "logs/graph.html"), 10, 15, TimeUnit.SECONDS);
		
			SingleTonSocketConn singleton=new SingleTonSocketConn();
			Thread t=new Thread(singleton);
			t.start();
		
			ReadResponse responseReaderThread=new ReadResponse();
			threadPool.execute(responseReaderThread);
			/*Thread reader=new Thread(responseReaderThread);
			reader.start();*/
			
			while (SERVER_RUNNING) { 
				try {
					executeTransaction();
				} catch (Exception e) {
					logger.log(Level.ERROR, e.getMessage(), e);
					e.printStackTrace();
				}
				Thread.sleep(HSQL_READ_DELAY);
			}
		} catch (Exception e) {
			logger.log(Level.ERROR, e.getMessage(), e);
		}
	}
	
	public static void executeTransaction() throws Exception
	{
		Object[] fields1 = mySqlConnection.readSAFTable();
		
		if(fields1 != null){
			for (int i = 0; i < fields1.length; i++) {
			Object[] columns = (Object[]) fields1[i];
				if(columns != null){
					logger.debug("columns ["+ columns+"]");
					final SAFTable saf=new SAFTable(fields1);
					logger.debug("threadPool.execute(saf)");
					threadPool.execute(saf);
		}
			}
		}
		
		Object[] fields = mySqlConnection.readInTableRequest();
		Object[] columns = null;
		if (fields != null) {
			for (int i = 0; i < fields.length; i++) {
				 columns = (Object[]) fields[i];
				if (columns != null) {
					int transactionNumber = Integer.valueOf(columns[0].toString()).intValue();							
					String entryTimeString = columns[5].toString();
					
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					Date entryTime = sdf.parse(entryTimeString);
					Calendar now = Calendar.getInstance();
					now.setTime(entryTime);
					now.add(Calendar.SECOND, STALE_TRANSACTION_TIMEOUT);
					Date staleTimeout = now.getTime();								
					
					if (Calendar.getInstance().getTime().after(staleTimeout)) {
						logger.log(Level.WARN, "STALE TRANSACTION " + transactionNumber + " entry time " + entryTime + " pickup time " + Calendar.getInstance().getTime());
						
						mySqlConnection.deleteFromIntable(transactionNumber);
					} else {
						TransactionProcessor transactionProcessor = new TransactionProcessor(columns);
						logger.debug("columns ["+ columns+"]");
						logger.debug("threadPool.execute(transactionProcessor)");
						threadPool.execute(transactionProcessor);	
					}
					mySqlConnection.deleteFromIntable(transactionNumber);
				}
			}
		}

	}
}
