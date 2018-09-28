package com.xxxxxx.transaction;

import java.io.FileInputStream;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.TimeZone;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.swing.text.StyledEditorKit.ForegroundAction;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.xxxxxx.encryption.des3ede.DES3ede;
import com.xxxxxx.iso8583handler.ISO8583Handler;

public class TGSocketReader implements Runnable {
	private Socket tgSocket;
	private static ThreadPoolExecutor threadPool = null;
	private static final int THREAD_POOL_SIZE = 1000;
	private static final int MAX_POOL_SIZE = 5000;
	private static final long KEEP_ALIVE_TIME = 15;

	
	private static final ArrayBlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(
			5000);
	private static Logger logger = Logger.getLogger(TGSocketReader.class);

	TGSocketReader(Socket tgSocket) {
		this.tgSocket = tgSocket;
		threadPool = new ThreadPoolExecutor(THREAD_POOL_SIZE, MAX_POOL_SIZE,
				KEEP_ALIVE_TIME, TimeUnit.SECONDS, queue);
	}

	@Override
	public void run() {
		Calendar now = Calendar.getInstance();
		now.add(Calendar.SECOND, 140);
		Date tgTimeout = now.getTime();

		
		try {
			while (isSocketConnected() && Calendar.getInstance().getTime().before(tgTimeout)) {
				if (tgSocket.getInputStream().available() > 0) {
					byte[] isoResponse = new byte[1000];
					tgSocket.getInputStream().read((byte[]) isoResponse);
					System.out.println("Reset timer");
					now = Calendar.getInstance();
					now.add(Calendar.SECOND, 1450);
					tgTimeout = now.getTime();
					
					String length = String.format("%02X",
							((byte[]) isoResponse)[0]);
					length += String.format("%02X", ((byte[]) isoResponse)[1]);
					int len = Integer.parseInt(length, 16);
					byte[] responseArray = Arrays.copyOf(
							((byte[]) isoResponse), len + 2);
					String str="";
					for (int i = 0; i < responseArray.length; i++) {
					 str += String.format("%02x", responseArray[i]);
					 System.out.print( responseArray[i]);
					}
					WorkerThread wt = new WorkerThread(responseArray);
					threadPool.execute(wt);
				}
			}
			
			//TransactionManager.setSocketBroken(true);
			//System.out.println("Socket connection broken, notifying TM...");
			//logger.log(Level.ERROR, "Socket connection broken, notifying TM...");
			
			
		} catch (Exception e) {
			logger.log(Level.ERROR,"Exception while creating tg socket", e);
			e.printStackTrace();
		}
	}
	
	private boolean isSocketConnected() {
		return !tgSocket.isClosed() && tgSocket.isConnected() && !tgSocket.isInputShutdown() && !tgSocket.isOutputShutdown();
	}
}

class WorkerThread implements Runnable{

	public static final int LOG_ON_RESPONSE = 22;
	public static final int ECHO_RESPONSE = 23;
	public static final int LOG_OFF_RESPONSE = 24;
	public static final int CUTOVER_RESPONSE = 26;
	private static final String LOGON_NETWORK_MGMT_INGO_CODE = "001";
	private static final String LOGOFF_NETWORK_MGMT_INGO_CODE = "002";
	private static final String ECHO_NETWORK_MGMT_INGO_CODE = "301";
	private static final String DOUBLE_LENGTH_KEY_NETWORK_MGMT_INGO_CODE = "162";
	private static final String CUTOVER_NETWORK_MGMT_INGO_CODE = "201";
	private static Logger logger = Logger.getLogger(WorkerThread.class);
	private byte[] responseArray;
	StringRequestBuilder stringRequestBuilder = new StringRequestBuilder();	
	WorkerThread(byte[] responseArray){
		this.responseArray = responseArray;		
	}
	private String asciiToCharString(String message){
		String temp="";
		for(int i=0;i< message.length();i+=2){
			String str = message.substring(i,i+2);
			temp += (char) Integer.parseInt(str, 16) ;
		}
		return temp;
	}
	@Override
	public void run() {
		Properties properties = new Properties();
		try {
			properties
					.load(new FileInputStream("ConnectionString.ini"));
			String isoXmlPath = properties.getProperty("ISO_XML_PATH1");
			String isoDtdPath = properties.getProperty("ISO_DTD_PATH1");
			
			System.out.println("SCT TO xxxxxx Header");
			String str="";
			for (int i = 0; i < responseArray.length; i++) {
			 str += String.format("%02x", responseArray[i]);
			 System.out.print( responseArray[i]);
			}
			System.out.println("SCT TO xxxxxx Header check " + str);
			String res=asciiToCharString(str);
			System.out.println("SCT TO xxxxxx ASCII TO CHAR : " + res);
			
			
			
			ISO8583Handler iso8583Handler = new ISO8583Handler(isoXmlPath, isoDtdPath, "SCT", "ASCII");
			String stringResponse = iso8583Handler.parse(res);

			System.out.println("parse response---"+stringResponse);
			Date receivedTime = new Date(System.currentTimeMillis());
			SimpleDateFormat sdf1 = new SimpleDateFormat("MMdd HHmmss");
			sdf1.setTimeZone(TimeZone.getTimeZone("GMT"));
			String receivedTimestamp = sdf1.format(receivedTime);	
			
			logger.log(Level.INFO,receivedTimestamp + " Incoming Message in TG SOCKET Reader " + stringResponse);
			System.out.println(receivedTimestamp + " SCT TO xxxxxx Response");
			
			
			String mti = stringRequestBuilder.getmti(stringResponse);
			String stan = stringRequestBuilder.getSystemTraceAuditNo(stringResponse);
			
			HSQLConnection hsqlConnection = new HSQLConnection();
			String pCode = "";
			String RRN = "";
			String authCode = "";
			String responseCode = "";
			String lastFourDigit = "";
			String status = "";
			String transactionTimestamp = "";
			long transactionNumber = 0;
			int messageType = 0;
			boolean insert = true;
			
			Date transactionTime = new Date(System.currentTimeMillis());
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			transactionTimestamp = sdf.format(transactionTime);	
			
			if(mti.equalsIgnoreCase("0210") || mti.equalsIgnoreCase("0430")){
				pCode = stringRequestBuilder.getProcessingCode(stringResponse);
				RRN = stringRequestBuilder.getRRN(stringResponse);
				
				transactionNumber = Integer.parseInt(stan);
				authCode = stringRequestBuilder.getAuthCode(stringResponse);
				responseCode = stringRequestBuilder.getResponseCode(stringResponse);
				//lastFourDigit = stringRequestBuilder.getLastFourDigit(stringResponse);	
				status = "00";
			}else if(mti.equalsIgnoreCase("0800")){
				
				status = "" + ISO8583Handler.ECHO_REQUEST;
				
				if (stringResponse
						.contains("|070|"
								+ ECHO_NETWORK_MGMT_INGO_CODE
								+ "|")) {
					status = "" + ECHO_RESPONSE;
					
				} else if (stringResponse.contains("|070|"
						+ CUTOVER_NETWORK_MGMT_INGO_CODE
						+ "|")) {
					status = "" + CUTOVER_RESPONSE;
					
				} else if (stringResponse.contains("|070|"
						+ LOGON_NETWORK_MGMT_INGO_CODE
						+ "|")) {
					status = "" + ECHO_RESPONSE;
					
				} else if (stringResponse.contains("|070|"
						+ LOGOFF_NETWORK_MGMT_INGO_CODE
						+ "|")) {
					status = "" + LOG_OFF_RESPONSE;
					
				}

				transactionNumber = Integer.parseInt(stan);
				messageType = 1;				
			}else if(mti.equalsIgnoreCase("0820")){
				status = "" + ISO8583Handler.ECHO_REQUEST;
				if (stringResponse
						.contains("|070|"
								+ ECHO_NETWORK_MGMT_INGO_CODE
								+ "|")) {
					status = "" + ECHO_RESPONSE;
					
				} else if (stringResponse.contains("|070|"
						+ CUTOVER_NETWORK_MGMT_INGO_CODE
						+ "|")) {
					status = "" + CUTOVER_RESPONSE;
					
				} else if (stringResponse.contains("|070|"
						+ LOGON_NETWORK_MGMT_INGO_CODE
						+ "|")) {
					status = "" + LOG_ON_RESPONSE;
					
				} else if (stringResponse.contains("|070|"
						+ LOGOFF_NETWORK_MGMT_INGO_CODE
						+ "|")) {
					status = "" + LOG_OFF_RESPONSE;
					
				}
				transactionNumber = Integer.parseInt(stan);
				messageType = 1;				
			}else if(mti.equalsIgnoreCase("0810")){
				status = "" + ISO8583Handler.ECHO_REQUEST;
				if (stringResponse
						.contains("|070|"
								+ ECHO_NETWORK_MGMT_INGO_CODE
								+ "|")) {
					status = "" + ECHO_RESPONSE;
					
				} else if (stringResponse.contains("|070|"
						+ CUTOVER_NETWORK_MGMT_INGO_CODE
						+ "|")) {
					status = "" + CUTOVER_RESPONSE;
					
				} else if (stringResponse.contains("|070|"
						+ LOGON_NETWORK_MGMT_INGO_CODE
						+ "|")) {
					status = "" + LOG_ON_RESPONSE;
					
				} else if (stringResponse.contains("|070|"
						+ LOGOFF_NETWORK_MGMT_INGO_CODE
						+ "|")) {
					status = "" + LOG_OFF_RESPONSE;
					
				}
				transactionNumber = Integer.parseInt(stan);
				messageType = 1;				
			}else if(mti.equalsIgnoreCase("0821")){
				status = "" + ISO8583Handler.ECHO_REQUEST;
				if (stringResponse
						.contains("|070|"
								+ ECHO_NETWORK_MGMT_INGO_CODE
								+ "|")) {
					status = "" + ECHO_RESPONSE;
					
				} else if (stringResponse.contains("|070|"
						+ CUTOVER_NETWORK_MGMT_INGO_CODE
						+ "|")) {
					status = "" + CUTOVER_RESPONSE;
					
				} else if (stringResponse.contains("|070|"
						+ LOGON_NETWORK_MGMT_INGO_CODE
						+ "|")) {
					status = "" + LOG_ON_RESPONSE;
					
				} else if (stringResponse.contains("|070|"
						+ LOGOFF_NETWORK_MGMT_INGO_CODE
						+ "|")) {
					status = "" + LOG_OFF_RESPONSE;
					
				}
				transactionNumber = Integer.parseInt(stan);
				messageType = 1;				
			}else{
				logger.error("Unknown message Error");
				insert = false;
			}
			
			if(insert) {
				System.out.println("insert in outtable"+stringResponse);
				hsqlConnection.writeOutTableResponse(transactionNumber, responseCode,  status, transactionTimestamp,1, stringResponse,"SCT","99310020" );
			}			
		} catch (Exception e) {
			logger.log(Level.ERROR, e.getMessage(), e);
			e.printStackTrace();
		}		
	}	
}
