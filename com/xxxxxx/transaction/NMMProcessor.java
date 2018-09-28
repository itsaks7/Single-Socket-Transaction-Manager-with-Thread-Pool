package com.xxxxxx.transaction;

import java.io.*;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;




//import com.xxxxxx.iso93.*;
import com.xxxxxx.encryption.des3ede.*;
import com.xxxxxx.iso8583handler.ISO8583Handler;

/**
 * This class is responsible for taking SALE,VOID,REFUND Transaction type of
 * ISORequest and process them. modified by pradnyag
 * 
 */
public class NMMProcessor implements Runnable {

	private static final int RESPONSE_TIMEOUT = 60;

	private static OutputStream outputStream = null;
	private static InputStream inputStream = null;
	private Socket socket = null;
	private Object[] fields = null;
	private static String transactionType = "";
	private static String transactionStatus = "";
	private boolean inProcess = true;
	private int transactionNumber = 0;
	private static final int TRANSACTION_TYPE_SALE = 1;
	private static final int TRANSACTION_TYPE_VOID = 2;
	private static final int TRANSACTION_TYPE_REFUND = 4;
	private static final int TRANSACTION_TYPE_PREAUTHORIZATION = 12;
	private static final int TRANSACTION_TYPE_AUTHORIZATION = 13;
	private static final int TRANSACTION_TYPE_PREAUTHORIZATION_COMPLETE = 19;
	private static final int TRANSACTION_TYPE_CASH_ADVANCE = 20;
	private static final int TRANSACTION_TYPE_TIP = 21;
	private int elapsedTimeOfISOBuildEnd = 0;
	private int elapsedTimeOfISOReceivedInTime = 0;
	private int elapsedTimeOfISOParseEnd = 0;
	private int elapsedTimeofWorkerThread = 0;
	private int elapsedTimeOfWriteOuttableEnd = 0;
	private static String STATUS_RESPONSE_SUCCESS = "00";
	private static final String STATUS_RESPONSE_TIMEOUT = "08";
	private static final String STATUS_RESPONSE_ERROR = "09";
	private static int transactionAmount = 0;
	private static final String OUTPUT_BYTE = "BYTE";
	private static final String OUTPUT_ASCII = "ASCII";
	private static String isoXmlPath = null;
	private static String isoDtdPath = null;
	public static final String xxxxxx_HEADER = "xxxxxxHEADER=";
	Logger logger = Logger.getLogger("com.xxxxxx.logger");
	private Socket tgSocket; 
	/**
	 * consructor of TransactionProcessor
	 * 
	 * @param fields
	 * @throws Exception
	 */
	public NMMProcessor(Object[] fields, Socket tgSocket) throws Exception {
		this.fields = fields;
		this.tgSocket = tgSocket;
		inProcess = true;
	}

	/**
	 * Take SALE ,VOID,REFUND type of ISO Request , other fields from INTABLE
	 * table(hsql), build it in ISO 8583 format them send it to TG and recieving
	 * ISOResponse from Acquirer and put required fields of ISOResponse in
	 * OUTTABLE.
	 */
	
	private String asciiToCharString(String message){
		String temp="";
		for(int i=0;i< message.length();i+=2){
			String str = message.substring(i,i+2);
			temp += (char) Integer.parseInt(str, 16) ;
		}
		return temp;
	}
	
	public synchronized void run() {
		if (inProcess) {
			try {
				StopWatch workerThreadWatch = new Log4JStopWatch(logger, Level.INFO);
				workerThreadWatch.start("WORKER THREAD", "START");
				Date workerThreadStartTime = new Date(System.currentTimeMillis());
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				String workerThreadTimestamp = df.format(workerThreadStartTime);
				
				/**
				 * 0 TRANSACTION_NUMBER INTEGER PRIMARY KEY, 1 REQUEST
				 * VARCHAR(1000), 2 STATUS varchar(2), 3 TG_ID VARCHAR(20), 4
				 * PORT INTEGER, 5 IP_ADDRESS VARCHAR(20), 6 ENTRY_TIME,7
				 * TG_NAME,8 OUTPUT_TYPE TIMESTAMP
				 */
				transactionNumber = Integer.valueOf(fields[0].toString()).intValue();

				String req = fields[1].toString();
				int requestType = Integer.parseInt(fields[2].toString());
				String ipAddress = fields[4].toString();
				int portNumber = Integer.valueOf(fields[3].toString()).intValue();
				
				HSQLConnection hsqlConnection = new HSQLConnection();
				hsqlConnection.deleteFromIntable(transactionNumber);
				
				String stringRequest = req;
				// String stringRequest = req;
				String tgName = fields[6].toString();
				String outputType = fields[7].toString();
								
				logger.log(Level.INFO,"xxxxxx To SCT NMM: " + stringRequest);
				System.out.println("xxxxxx To SCT NMM: " + stringRequest);
				
				StopWatch debugWatch = new Log4JStopWatch(logger, Level.DEBUG);
								
				Properties properties = new Properties();
				try {
					properties.load(new FileInputStream("ConnectionString.ini"));
				} catch (Exception e) {
					logger.log(Level.ERROR, e.getMessage(), e);
				}
				isoXmlPath = properties.getProperty("ISO_XML_PATH1");
				isoDtdPath = properties.getProperty("ISO_DTD_PATH1");
				
				ISO8583Handler iso8583Handler = new ISO8583Handler(isoXmlPath, isoDtdPath, tgName, outputType, requestType);

				if (outputType.equals(OUTPUT_ASCII)) {
					OutputStream out = tgSocket.getOutputStream();
					//Build ISO message from string request
					byte[] isoRequest = (byte[]) iso8583Handler.build(stringRequest);
					String str="";
					for (int i = 0; i < isoRequest.length; i++) {
					 str += String.format("%02x", isoRequest[i]);
					 System.out.print( isoRequest[i]);
					}
					String res=asciiToCharString(str);
					System.out.println("SCT TO xxxxxx ASCII TO CHAR : " + res);
					System.out.println("build"+isoRequest);
					try {
						out.write(isoRequest);
					} catch (Exception e) {
						e.printStackTrace();
						//TransactionManager.setSocketBroken(true);
					}
					out.flush();
					inProcess = false;
				}
			}catch(Exception e){
				logger.error("Exception In NMM Processor", e);
				e.printStackTrace();
			}
		}
	}
}