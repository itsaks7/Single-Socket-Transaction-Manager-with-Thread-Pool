package com.xxxxxx.transaction;

import java.io.*;
import java.net.InetAddress;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.print.DocFlavor.STRING;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;

import com.xxxxxx.iso8583handler.ISO8583Handler;
//import com.xxxxxx.iso93.*;
import com.xxxxxx.encryption.des3ede.*;

/**
 * This class is responsible for taking SALE,VOID,REFUND Transaction type of
 * ISORequest and process them. modified by parag
 * 
 */
public class SCTProccessor implements Runnable {

	private static final int RESPONSE_TIMEOUT = 30;

	private static OutputStream outputStream = null;
	private static InputStream inputStream = null;
	private Socket socket = null;
	private Object[] fields = null;
	private static String transactionType = "";
	private static String transactionStatus = "";
	private boolean inProcess = true;
	private Long transactionNumber;
	private static final int TRANSACTION_TYPE_SALE = 1;
	private static final int TRANSACTION_TYPE_EMV_SALE=20;
	private static final int TRANSACTION_TYPE_EMV_PINBLOCK_SALE=21;
	private static final int TRANSACTION_TYPE_VOID = 2;
	private static final int TRANSACTION_TYPE_REFUND = 4;
	private static final int TRANSACTION_TYPE_REVERSAL = 5;
	private static final int TRANSACTION_TYPE_PREAUTHORIZATION = 12;
	private static final int TRANSACTION_TYPE_AUTHORIZATION = 13;
	private static final int TRANSACTION_TYPE_PREAUTHORIZATION_COMPLETE = 19;
	private static final int TRANSACTION_TYPE_CASH_ADVANCE = 23;
	private static final int TRANSACTION_TYPE_TIP = 8;
	private static final int TRANSACTION_TYPE_SALE_COMPLETION = 7;
	private static final int TRANSACTION_TYPE_BATCH_UPLOAD_REQUEST = 11;
	private static final int TRANSACTION_TYPE_SETTLEMENT = 3;
	private static final int TRANSACTION_TYPE_SETTLEMENT_TRAILER= 9;
	private static final int TRANSACTION_TYPE_KEY_EXCHANGE = 50;
	private static final int TRANSACTION_TYPE_TMK_Download =51;
	private static final int TRANSACTION_TYPE_Key_Exchange_with_line =52;
	private static final int TRANSACTION_TYPE_Key_Exchange_without_line =53;
	private static final int TRANSACTION_TYPE_Key_Parameter_download =54;
	private int elapsedTimeOfISOBuildEnd = 0;
	private int elapsedTimeOfISOReceivedInTime = 0;
	private int elapsedTimeOfISOParseEnd = 0;
	private int elapsedTimeofWorkerThread = 0;
	private int elapsedTimeOfWriteOuttableEnd = 0;
	private static  String STATUS_RESPONSE_SUCCESS = "00";
	private static final String STATUS_RESPONSE_TIMEOUT = "97";
	private static final String STATUS_RESPONSE_ERROR = "09";
	private static String transactionAmount = "";
	private static final String OUTPUT_BYTE = "BYTE";
	private static final String OUTPUT_ASCII = "ASCII";
	private static final String OUTPUT_STRING = "STRING";
	private static final String OUTPUT_BCD = "BCD";
	private static ThreadPoolExecutor threadPool1 = null;
	private static final int THREAD_POOL_SIZE = 1000;
	private static final int MAX_POOL_SIZE = 5000;
	private static final long KEEP_ALIVE_TIME = 15;
	private static final ArrayBlockingQueue<Runnable> queue1 = new ArrayBlockingQueue<Runnable>(
			5000);
	private static final String lmk = Key.getKeyfromFile("junk.ini");
	private static String isoXmlPath = null;
	private static String isoDtdPath = null;
	Logger logger = Logger.getLogger(SCTProccessor.class.getName());

	private Object threadPool;

	private Object transactionProcessor;

	private BlockingQueue<Runnable> queue;

	/**
	 * constructor of TransactionProcessor
	 * 
	 * @param fields
	 * @param socketSCT 
	 * @throws Exception
	 */
	public SCTProccessor(Object[] fields, Socket socketSCT) throws Exception {
		this.fields = fields;
		this.socket = socketSCT;
		inProcess = true;
			}

	
	/*
	 * method for conversion of hexa string iso message to bytearray form
	 *  */ 
	public static byte[] hexStringToByteArray(String s) {
		   int len = s.length();
		   byte[] data = new byte[len / 2];
		   for (int i = 0; i < len-1; i += 2) {
		       data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
		                            + Character.digit(s.charAt(i+1), 16));
		   }
		   return data;
		}
	
	private String asciiToCharString(String message){
		String temp="";
		for(int i=0;i< message.length();i+=2){
			String str = message.substring(i,i+2);
			temp += (char) Integer.parseInt(str, 16) ;
		}
		return temp;
	}
	/**
	 * Take SALE ,VOID,REFUND type of ISO Request , other fields from INTABLE
	 * table(hsql), build it in ISO 8583 format them send it to TG and recieving
	 * ISOResponse from Acquirer and put required fields of ISOResponse in
	 * OUTTABLE.
	 */
	public synchronized void run() {
		if (inProcess) {
			try {

				StopWatch workerThreadWatch = new Log4JStopWatch(logger, Level.INFO);
				workerThreadWatch.start("WORKER THREAD", "START");
				Date workerThreadStartTime = new Date(System.currentTimeMillis());
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
				String workerThreadTimestamp = df.format(workerThreadStartTime);

				
				transactionNumber = (long) Integer.valueOf(fields[0].toString()).intValue();
				 
				String req = fields[1].toString();//pipe separated iso request in encrypted form using lmk. 
				int requestType = Integer.parseInt(fields[2].toString());//request type eg.sale void refund
				String ipAddress = fields[4].toString();//IP address of TG
				int portNumber = Integer.valueOf(fields[3].toString()).intValue();//port number of TG
				
				String stringRequest = DES3ede.DD(lmk, req);//decrypted String request using lmk
				//String stringRequest = req;
				String tgName = fields[6].toString();//TG name 
				String outputType = fields[7].toString();//OUTPUT type of TG eg. String BCD ASCII.
				
				
				logger.debug("---------Txn id***"+transactionNumber);
				logger.debug("---------Request type***"+requestType);
				logger.debug("---------IPAddress***"+ipAddress);
				logger.debug("---------StringRequest***"+stringRequest);
				
				StopWatch debugWatch = new Log4JStopWatch(logger, Level.DEBUG);
				  
				
				StringRequestBuilder requestBuilder = new StringRequestBuilder();
				if (requestType == TRANSACTION_TYPE_SALE 
						|| requestType == TRANSACTION_TYPE_PREAUTHORIZATION
						|| requestType == TRANSACTION_TYPE_REVERSAL
						|| requestType == TRANSACTION_TYPE_REFUND
						|| requestType == TRANSACTION_TYPE_SALE_COMPLETION
						) {
					transactionAmount = requestBuilder.getAmount(stringRequest); //transaction amount in field no 004
				}
				
				Properties properties = new Properties();
				
				try {
					properties.load(new FileInputStream("ConnectionString.ini"));
				} catch (Exception e) {
					e.printStackTrace();
				}
				isoXmlPath = properties.getProperty("ISO_XML_PATH1");//iso xml and dtd files  path
				isoDtdPath = properties.getProperty("ISO_DTD_PATH1");
				ISO8583Handler isoHandler = new ISO8583Handler(isoXmlPath, isoDtdPath, tgName,
						outputType, requestType);
				System.out.println(isoXmlPath+"--"+tgName+outputType+requestType);
				System.out.println("ip"+ipAddress+"---port"+portNumber);
				//socket = new Socket(ipAddress, portNumber);
				logger.debug("socket at***"+socket);
				if (outputType.equals(OUTPUT_BYTE)) {
					OutputStream outputStream = socket.getOutputStream();
					debugWatch.start("ISO BUILD START");
					byte[] ISORequest = (byte[]) isoHandler.build(stringRequest);
					debugWatch.stop("ISO BUILD END");
					elapsedTimeOfISOBuildEnd = (int) debugWatch.getElapsedTime();
					synchronized (outputStream) {
						
						outputStream.write(ISORequest);
						debugWatch.start("ISO SENT");
						outputStream.flush();
					}
				} else if (outputType.equals(OUTPUT_ASCII)) {
					OutputStream out = socket.getOutputStream();
					
					//Build ISO message from string request
					byte[] isoRequest = (byte[]) isoHandler.build(stringRequest);
					
					String str="";
					for (int i = 0; i < isoRequest.length; i++) {
					 str +=  String.format("%2x",isoRequest[i]);
					 System.out.print( isoRequest[i]);
					}
					System.out.println("xxxxxx To SCT Header check " + str);
					String res=asciiToCharString(str);
					System.out.println("xxxxxx To SCT  ASCII TO CHAR : " + res);
					
					try {
						out.write(isoRequest);
					} catch (Exception e) {
						e.printStackTrace();
						//TransactionManager.setSocketBroken(true);
					}
					out.flush();
					//inProcess = false;	
				}   
				/** changes for ATOS HDFC begins here ***/
					else if (outputType.equals(OUTPUT_STRING)) {
						
						logger.debug("ready to send");
						
						OutputStream out = socket.getOutputStream();
												
						String ISORequest = (String) isoHandler.build(stringRequest);
									
						logger.debug(ISORequest);
						elapsedTimeOfISOBuildEnd = (int) debugWatch.getElapsedTime();
						synchronized (out) {
						
						out.write(hexStringToByteArray(ISORequest));
						debugWatch.start("ISO SENT");
						logger.debug("request sent......");
						out.flush();
						}
				}
					else if (outputType.equals(OUTPUT_BCD)) {
						System.out.println("ready to send");
						System.out.println("ready to send BCD");
						OutputStream outputStream = socket.getOutputStream();
						debugWatch.start("ISO BUILD START");
					
						System.out.println(stringRequest);
						byte[] ISORequest = (byte[]) isoHandler.build(stringRequest);
						
						
						System.out.println("isoreq "+ISORequest);
						System.out.println("build Request "+ISORequest);
						debugWatch.stop("ISO BUILD END");
						elapsedTimeOfISOBuildEnd = (int) debugWatch.getElapsedTime();
						synchronized (outputStream) {

							outputStream.write(ISORequest);
							debugWatch.start("ISO SENT");
							System.out.println("sent.....");
							outputStream.flush();
						}
					}
				/** changes for ATOS ends here ***/
				Calendar now = Calendar.getInstance();
				now.add(Calendar.SECOND, RESPONSE_TIMEOUT);
				Date tgTimeout = now.getTime();
				
				boolean isTimedOut = true;
				HSQLConnection hSqlConnection = new HSQLConnection();
				switch (requestType) {
				case TRANSACTION_TYPE_SALE:
					transactionType = "Sale";
					break;
				case TRANSACTION_TYPE_VOID:
					transactionType = "Void";
					break;
				case TRANSACTION_TYPE_REFUND:
					transactionType = "Refund";
					break;
				case TRANSACTION_TYPE_PREAUTHORIZATION:
					transactionType = "PreAuthorization";
					break;
				case TRANSACTION_TYPE_AUTHORIZATION:
					transactionType = "Authorization";
					break;
				case TRANSACTION_TYPE_REVERSAL:
					transactionType = "Reversal";
					break;
				case TRANSACTION_TYPE_PREAUTHORIZATION_COMPLETE:
					transactionType = "PreAuthorization_complete";
					break;
				case TRANSACTION_TYPE_CASH_ADVANCE:
					transactionType = "Cash_Advance";
					break;
				case TRANSACTION_TYPE_TIP:
					transactionType = "Tip";
					break;
				case TRANSACTION_TYPE_KEY_EXCHANGE:
					transactionType = "Key_Exchange";
					break;
				case TRANSACTION_TYPE_TMK_Download:
					transactionType = "TMK_Download";
					break;
				case TRANSACTION_TYPE_Key_Exchange_with_line:
					transactionType = "Key_Exchange_with_line";
					break;
				case TRANSACTION_TYPE_Key_Exchange_without_line:
					transactionType = "Key_Exchange_without_line";
					break;
				case TRANSACTION_TYPE_SALE_COMPLETION:
					transactionType = "Sale_Completion";
					break;
				case TRANSACTION_TYPE_BATCH_UPLOAD_REQUEST:
					transactionType = "Batch_Upload";
					break;
				case TRANSACTION_TYPE_SETTLEMENT:
					transactionType = "Settlement";
					break;
				case TRANSACTION_TYPE_SETTLEMENT_TRAILER:
					transactionType = "Settlement_trailer";
					break;
				case TRANSACTION_TYPE_Key_Parameter_download:
					transactionType = "Parameter_download";
					break;
					
				}
				String parseMsg="";
				while (!socket.isClosed() && socket.isConnected() && !socket.isInputShutdown()
						&&Calendar.getInstance().getTime().before(tgTimeout)) {
					if (socket.getInputStream().available() > 0) {
						Object isoResponse = null;

						if (outputType.equals(OUTPUT_BYTE)) {
							isoResponse = new byte[1000];
							socket.getInputStream().read((byte[]) isoResponse);
						} else if (outputType.equals(OUTPUT_ASCII)) {
							
							BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
							isoResponse = "";
							if ((isoResponse = in.readLine()) != null) {
								System.out.println("response----"+isoResponse);
								parseMsg = isoHandler.parse(isoResponse);
								System.out.println("res from acq:"+parseMsg);
							}
						
						}
						/** changes for ATOS begins here ***/
						else if (outputType.equals(OUTPUT_STRING)) {
							
								byte[] isoResponse1 = new byte[1000];
								socket.getInputStream().read(isoResponse1);
								
							    String temp="";
								for (int i = 0; i < 2; i++) {
									temp+=String.format("%02x", isoResponse1[i]);
								}
								
								int length=(Integer.parseInt(temp, 16))+2;
								String build_response="";
								for(int j=0;j<length;j++){
									build_response+=String.format("%02x", isoResponse1[j]);
								}
								
								logger.debug("--------build response***"+build_response);								
				
								parseMsg = isoHandler.parse(build_response);
								
								logger.debug("-------Parsed Response***"+parseMsg);
						
						}
						
						else if (outputType.equals(OUTPUT_BCD)) {
							
							isoResponse = "";
							isoResponse = new byte[1000];
							socket.getInputStream().read((byte[]) isoResponse);
								
							
							
								parseMsg = isoHandler.parse(isoResponse);
								System.out.println("res from acq:"+parseMsg);
							
						
						}
						/** changes for ATOS ends  here ***/
						

						//
						debugWatch.stop("ISO Received in time");
						elapsedTimeOfISOReceivedInTime = (int) debugWatch.getElapsedTime();
						debugWatch.start("ISO PARSE START");
						String stringResponse = parseMsg;
					
						String responseCode = "";
						String returnAmount = requestBuilder.getAmount(stringResponse);
						if (returnAmount!=null && !transactionAmount.equals( returnAmount)) {
							responseCode = STATUS_RESPONSE_ERROR;
						} else {
							responseCode = requestBuilder.getResponseCode(stringResponse);
						}
						
						String response_message=stringResponse;
						
						String Response_Status=STATUS_RESPONSE_SUCCESS;
						
						String mti= requestBuilder.getmti(stringRequest);
						int response_mti=(Integer.parseInt(mti))+10;
						String new_mti= "0"+ Integer.toString(response_mti);
						
						String response_code_mti=requestBuilder.getmti(stringResponse);
						if(!response_code_mti.equalsIgnoreCase(new_mti)){
							response_message="Invalid Message Type";
							Response_Status=STATUS_RESPONSE_ERROR;
						}
						
						
						String pan_number=requestBuilder.getPanNumber(stringRequest);
						String response_pan_number=requestBuilder.getPanNumber(stringResponse);
						
						if((null!=pan_number && !pan_number.equalsIgnoreCase(""))&&
								(null!=response_pan_number && !response_pan_number.equalsIgnoreCase(""))
								&&!response_pan_number.equalsIgnoreCase(response_pan_number))
						{
							response_message="PAN Differs";
							Response_Status=STATUS_RESPONSE_ERROR;
						}
							
						String processing_code=requestBuilder.getProcessingCode(stringRequest);
						String response_processing_code= requestBuilder.getProcessingCode(stringResponse);
						
						if((null!=response_processing_code && !response_processing_code.equalsIgnoreCase("")) &&
								!response_processing_code.equalsIgnoreCase(processing_code)){
							response_message="proc. code Differs";
							Response_Status=STATUS_RESPONSE_ERROR;
						}
			
						String stan=requestBuilder.getSystemTraceAuditNo(stringRequest);
						String response_Stan=requestBuilder.getSystemTraceAuditNo(stringResponse);
						
						if((null!=response_Stan && !response_Stan.equalsIgnoreCase("")) && 
								!response_Stan.equalsIgnoreCase(stan)){
							response_message="Stan Mismatch";
							Response_Status=STATUS_RESPONSE_ERROR;
						}
						
						String NII=requestBuilder.getNII(stringRequest);
						String response_NII=requestBuilder.getNII(stringResponse);
						
						if((null!=response_NII && !response_NII.equalsIgnoreCase("")) && 
								!response_NII.equalsIgnoreCase(NII)){
							response_message="Invalid NII";
							Response_Status=STATUS_RESPONSE_ERROR;
						}
						
						String terminal_id=requestBuilder.getTerminalId(stringRequest);
						String response_terminal_id=requestBuilder.getTerminalId(stringResponse);
						
						if((null!=response_terminal_id && !response_terminal_id.equalsIgnoreCase("")) &&
								!response_terminal_id.equalsIgnoreCase(terminal_id))
						{
							response_message="Terminal ID Differs ";
							Response_Status=STATUS_RESPONSE_ERROR;
						}
						
						
						
						
						responseCode = requestBuilder.getResponseCode(stringResponse);
						
						
						int MESSAGE_TYPE = 1;
						debugWatch.stop("ISO PARSE END");
						elapsedTimeOfISOParseEnd = (int) debugWatch.getElapsedTime();
						Date transactionTime = new Date(System.currentTimeMillis());
						SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
						String transactionTimestamp = sdf.format(transactionTime);
						
					
						debugWatch.start("WRITE OUTTABLE START");
						
						
					
						hSqlConnection.writeOutTableResponse(transactionNumber,
								responseCode,Response_Status,
								transactionTimestamp, MESSAGE_TYPE, response_message ,tgName,terminal_id);
					
						
						threadPool1 = new ThreadPoolExecutor(THREAD_POOL_SIZE,
								MAX_POOL_SIZE, KEEP_ALIVE_TIME, TimeUnit.SECONDS, queue1);
						
						
						debugWatch.stop("WRITE OUTTABLE END");
						elapsedTimeOfWriteOuttableEnd = (int) debugWatch.getElapsedTime();
						workerThreadWatch.stop("WORKER THREAD", "WRITE SUCCESS");
						elapsedTimeofWorkerThread = (int) workerThreadWatch.getElapsedTime();
						String workerThreadEndTimestamp = sdf.format(transactionTime);

						transactionStatus = responseCode;
						if (responseCode.equalsIgnoreCase("00")) {
							transactionStatus = "Success";
						} else {
							transactionStatus = "Failure";
						}

						int timeDiff = (int) (transactionTime.getTime() - workerThreadStartTime
								.getTime());
                      
						hSqlConnection.insertRecordInLogRecord(transactionNumber, requestType,
								transactionStatus, elapsedTimeOfISOBuildEnd,
								elapsedTimeOfISOParseEnd, elapsedTimeOfISOReceivedInTime,
								elapsedTimeofWorkerThread, elapsedTimeOfWriteOuttableEnd,
								workerThreadTimestamp, workerThreadEndTimestamp, timeDiff);
						isTimedOut = false;
						break;
					}
					Thread.sleep(10);
				}

				if (isTimedOut) {
					transactionStatus = "Connection time out";
					
					Date transactionTime = new Date(System.currentTimeMillis());
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					String transactionTimestamp = sdf.format(transactionTime);

					int MESSAGE_TYPE=1;
					hSqlConnection.writeOutTableResponse(transactionNumber, "",
							 STATUS_RESPONSE_TIMEOUT, transactionTimestamp,MESSAGE_TYPE,"timeout",tgName,"");
					debugWatch.stop("WRITE OUTTABLE TIMEOUT");
					elapsedTimeOfWriteOuttableEnd = (int) debugWatch.getElapsedTime();

					long elapsedTime = workerThreadWatch.getElapsedTime();
					logger.debug("elapsed time of workerThreadWatch:" + elapsedTime);
					elapsedTimeofWorkerThread = (int) workerThreadWatch.getElapsedTime();
					String workerThreadEndTimestamp = sdf.format(transactionTime);
					int timeDiff = (int) (transactionTime.getTime() - workerThreadStartTime
							.getTime());

					hSqlConnection.insertRecordInLogRecord(transactionNumber, requestType,
							transactionStatus, elapsedTimeOfISOBuildEnd, elapsedTimeOfISOParseEnd,
							elapsedTimeOfISOReceivedInTime, elapsedTimeofWorkerThread,
							elapsedTimeOfWriteOuttableEnd, workerThreadTimestamp,
							workerThreadEndTimestamp, timeDiff);
				}
			} catch (Exception e) {
				logger.log(Level.ERROR, e.getMessage(), e);
				e.printStackTrace();
			} finally {
				try {
					if (inputStream != null && outputStream != null && socket != null) {
						inputStream.close();
						outputStream.close();
						socket.close();
						socket = null;
					}
				} catch (Exception e) {
					logger.log(Level.ERROR, e.getMessage(), e);
					e.printStackTrace();
				}
				inProcess = false;				
			}			
		}
	}
}