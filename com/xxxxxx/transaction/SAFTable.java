package com.xxxxxx.transaction;

import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;

import com.xxxxxx.encryption.des3ede.DES3ede;
import com.xxxxxx.encryption.des3ede.Key;
import com.xxxxxx.iso8583handler.ISO8583Handler;
import com.xxxxxx.socketClasses.ReadResponse;

public class SAFTable implements Runnable {

	private boolean inprocess=false;
	private static String isoXmlPath = null;
	private static String isoDtdPath = null;
	public static Socket socket = null;
	private Object[] fields1=null;
	Logger logger = Logger.getLogger("com.xxxxxx.logger");
	private int RESPONSE_TIMEOUT=30;
	long transactionNumber;


	public SAFTable(Object[] fields1)throws Exception{
		this.fields1=fields1;
		inprocess =true;

	}

	public static byte[] hexStringToByteArray(String s) {
		int len = s.length();
		byte[] data = new byte[len / 2];
		for (int i = 0; i < len-1; i += 2) {
			data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
					+ Character.digit(s.charAt(i+1), 16));
		}
		return data;
	}


	public synchronized void run() {
		if (inprocess) {
			try {

				StopWatch workerThreadWatch = new Log4JStopWatch(logger, Level.INFO);
				workerThreadWatch.start("WORKER THREAD", "START");

				if (fields1 != null) {
					logger.debug("No. of Rows :: "+fields1.length);
					for (int i = 0; i < fields1.length; i++) {
						Object[] columns = (Object[]) fields1[i];
						if (columns != null) {
							transactionNumber = (long) Integer.valueOf(columns[0].toString()).intValue();
							String req = columns[1].toString();
							int requestType = Integer.parseInt(columns[3].toString());
							final String lmk = Key.getKeyfromFile("junk.ini");
							String stringRequest = DES3ede.DD(lmk, req);
							String tgName = columns[7].toString();
							String outputType = columns[8].toString();
							StopWatch debugWatch = new Log4JStopWatch(logger, Level.DEBUG);

							try {
								socket = SingleTonSocketConn.getInstance();
								logger.debug("----Socket instance created from singleton ------ "+getClass());
								OutputStream outputStream = socket.getOutputStream();
								debugWatch.start("ISO BUILD START");
								debugWatch.stop("ISO BUILD END");
								try{

									outputStream.write(hexStringToByteArray(stringRequest));
									logger.debug("******************SENT SUCCESSFULLY*********************");

								}catch(SocketException e){
									logger.debug("----Socket Exeption from ------ "+getClass());
									e.printStackTrace();
									if(socket!=null){
										socket.close();
										socket=null;
										logger.debug("----Closing Socket connection due to SocketException-----"+getClass());
										socket = SingleTonSocketConn.getInstance();	
									}

								}catch(Exception e){
									logger.debug("----Other Exeption from ------ "+getClass());
									e.printStackTrace();
									if(socket!=null){
										socket.close();
										logger.debug("socket *****"+ socket==null);
										socket=null;
										logger.debug("***closing socket");
									}
									socket = SingleTonSocketConn.getInstance();
								}	

								/** changes for ATOS ends here ***/
								Calendar now = Calendar.getInstance();
								now.add(Calendar.SECOND, RESPONSE_TIMEOUT);
								Date tgTimeout = now.getTime();
								//new ReadResponse(transactionNumber,tgTimeout,outputType,columns);
								inprocess=false;
								break;

							}catch(Exception e){
								logger.log(Level.ERROR, e.getMessage(), e);
							}
						}
					}
				}
			} catch (Exception e) {
				logger.log(Level.ERROR, e.getMessage(), e);
				e.printStackTrace();

			} finally {
				inprocess = false;

			}

		}
	}


}