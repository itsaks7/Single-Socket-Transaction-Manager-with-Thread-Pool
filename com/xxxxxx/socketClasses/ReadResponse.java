package com.xxxxxx.socketClasses;

import java.io.DataInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;
import org.jpos.iso.ISOUtil;
import org.jpos.iso.packager.GenericPackager;
import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;

import com.mosambee.transaction.Convert;
import com.mosambee.transaction.MYSQLConnection;
import com.mosambee.transaction.SingleTonSocketConn;
import com.mosambee.transaction.StringRequestBuilder;

public class ReadResponse implements Runnable{
	public static String ipAddress = null;
	public static int port;
	public static Date tgTimeout = null;
	public static String isoXmlPath = null;
	public static String isoDtdPath = null;
	public static String stringRequest ="";
	private static StringRequestBuilder requestBuilder;
	private static long transactionNumber;
	String tgName = "";
	int requestType;
	String outputType;
	private static final String OUTPUT_ASCII = "ASCII";
	private static  String STATUS_RESPONSE_SUCCESS = "00";
	private static final String STATUS_RESPONSE_TIMEOUT = "97";
	MYSQLConnection mySqlConnection=new MYSQLConnection();
	ISOMsg response = new ISOMsg();
	boolean isTimedOut=true;
	boolean inprocess = true;
	int counter =0;
	String transactionStatus="UNKNOWN";
	Object[] fields;
	String terminald;
	public ReadResponse(){};

	String pipeSaperatedParsedData="";


	Logger logger = Logger.getLogger(ReadResponse.class.getName());

	public void run() {

		logger.debug("Current Thread ID " +Thread.currentThread().getId());
		//terminald=fields[8].toString();
		StopWatch debugWatch = new Log4JStopWatch(logger, Level.DEBUG);
		while(inprocess)
		{
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e1) {
				e1.printStackTrace();
			}
			while(SingleTonSocketConn.getInstance()!=null)
			{	
				try {
					while(!SingleTonSocketConn.getInstance().isClosed() && SingleTonSocketConn.getInstance().isConnected() && !SingleTonSocketConn.getInstance().isInputShutdown()
							) {
						if(SingleTonSocketConn.getInputStream().available() > 0) {
							byte[] isoResponse=null;
							DataInputStream bin = new DataInputStream(SingleTonSocketConn.getInstance().getInputStream());
							synchronized (bin) {
								isoResponse = new byte[1000];
								bin.read(isoResponse);
							}
							String isoResponseHEX=ISOUtil.hexString(isoResponse);
							String length=isoResponseHEX.substring(0,4);
							int lengthint=Integer.parseInt(length, 16)*2;
							isoResponseHEX=isoResponseHEX.substring(4,lengthint+4);
							System.out.println("BYTES TO HEX UNFORMATTED = > "+ISOUtil.hexString(isoResponse));
							System.out.println("BYTES TO HEX Formatted = > "+isoResponseHEX);
							logger.debug("******************READ SUCCESFULLY*********************");
							ISOMsg jposresponse =new ISOMsg();
							GenericPackager packager = new GenericPackager("iso87binary.xml");
							jposresponse.setPackager(packager);
							byte [] arrayByte=Arrays.copyOfRange(isoResponse, 2, isoResponse.length);

							jposresponse.unpack(arrayByte);

							Date transactionTime = new Date(System.currentTimeMillis());
							SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
							String transactionTimestamp = sdf.format(transactionTime);
							debugWatch.stop("ISO Received in time");

							String jposParsedResponse = logISOMsg(jposresponse);
							logger.debug("Response :: "+jposParsedResponse);

							if(jposresponse.getMTI().equals("0800"))
							{
								if(jposresponse.getString("070").equals("001"))
								{
									sendSignOn(jposresponse);
								}
								else if(jposresponse.getString("070").equals("001"))
								{
									sendEcho(jposresponse);
								}
							}
							else
							{

								pipeSaperatedParsedData=logISOMsg(jposresponse);

								mySqlConnection.writeOutTableResponse(transactionNumber,
										"",  STATUS_RESPONSE_SUCCESS,
										transactionTimestamp,0, isoResponseHEX , "MET", terminald);	
							}
						}
						isTimedOut=false;
					}
				}catch (Exception e) {
					e.printStackTrace();

				}

			}
		}
	}
	private String logISOMsg(ISOMsg msg) {
		String isoResponse = "";
		String EMVarray[] = null;
		StringBuilder dE127ISOresponse=new StringBuilder("");
		logger.info("----ISO MESSAGE-----");
		try {
			logger.debug("    MTI : " + msg.getMTI());
			isoResponse =  "|"+msg.getMTI();
			for (int i=1;i<=msg.getMaxField();i++) {
				if (msg.hasField(i)) {
					if(i!=127)
						logger.debug(" Field-"+i+" : "+msg.getString(i));
					String bitNum = String.format("%03d", i);
					String sf127="";
					if(i!=127)
						isoResponse += "|" + bitNum + "|" + msg.getString(i);
					if(i==127)
					{	

						for(int sf=1;sf<39;sf++)
						{
							if(sf<25)
							{
								sf127=msg.getString("127."+String.valueOf(sf));
								if(sf127!=null)
								{
									logger.debug("Field 127 SF :" +sf+"-->"+sf127);
									dE127ISOresponse.append(sf127);
								}	
							}
							if(sf==25)
							{
								sf127=msg.getString("127."+String.valueOf(sf)+".1");
								if(sf127!=null)
								{
									EMVarray[0]=msg.getString("127."+String.valueOf(sf)+".6");
									EMVarray[1]=msg.getString("127."+String.valueOf(sf)+".10");
									EMVarray[2]=msg.getString("127."+String.valueOf(sf)+".31");
									EMVarray[3]=msg.getString("127."+String.valueOf(sf)+".32");
									EMVarray[4]=msg.getString("127."+String.valueOf(sf)+".33");
								}

								if(EMVarray!=null)
								{	
									logger.debug("Field 127 SF 25 SF EMV:" +"6"+"-->"+EMVarray[0]);
									logger.debug("Field 127 SF 25 SF EMV:" +"10"+"-->"+EMVarray[1]);
									logger.debug("Field 127 SF 25 SF EMV:" +"31"+"-->"+EMVarray[2]);
									logger.debug("Field 127 SF 25 SF EMV:" +"32"+"-->"+EMVarray[3]);
									logger.debug("Field 127 SF 25 SF EMV:" +"33"+"-->"+EMVarray[4]);
									dE127ISOresponse.append(EMVarray[0]
											+":"+EMVarray[1]
													+":"+EMVarray[2]
															+":"+EMVarray[3]
																	+":"+EMVarray[4]);
								}
							}

							if(sf>25)
							{
								sf127=msg.getString("127."+String.valueOf(sf));
								if(sf127!=null)
								{
									logger.debug("Field 127 SF :" +sf+"-->"+sf127);
									dE127ISOresponse.append(sf127);
								}	
							}
						}
						if(i==127)
						{	
							logger.debug(" Field-"+i+" : "+dE127ISOresponse.toString());
							isoResponse += "|" + bitNum + "|"+dE127ISOresponse.toString();
						}
					}
				}
			}
		} catch (ISOException e) {
			e.printStackTrace();
		}
		return isoResponse + "|";
	}

	public void sendSignOn(ISOMsg receivedISO) throws ISOException
	{
		ISOMsg isoRequest =new ISOMsg();
		GenericPackager packager = new GenericPackager("iso87binary.xml");
		isoRequest.setPackager(packager);
		isoRequest.setMTI("0810");
		isoRequest.set("007",receivedISO.getString("007"));
		isoRequest.set("011",receivedISO.getString("011"));
		isoRequest.set("012",receivedISO.getString("012"));
		isoRequest.set("013",receivedISO.getString("013"));
		isoRequest.set("039","00");
		isoRequest.set("070","001");

		byte[] pakedBytes= isoRequest.pack();
		logger.info("HEX ISO REQUEST : "+ISOUtil.hexString(pakedBytes));
		logISOMsg(isoRequest);
		String messageString = ISOUtil.hexString(pakedBytes);
		String lengthStr =StringUtils.leftPad(Integer.toHexString(messageString.length() / 2),4, "0");
		messageString = lengthStr + messageString;

		try {
			SingleTonSocketConn.getOutputStream().write(Convert.hexStringToByteArray(messageString));
			logger.info("-------------ISO WRITTEN SUCCESSFULLY---------------");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void sendEcho(ISOMsg receivedISO) throws ISOException
	{
		ISOMsg isoRequest =new ISOMsg();
		GenericPackager packager = new GenericPackager("iso87binary.xml");

		isoRequest.setMTI("0810");
		isoRequest.set("007",receivedISO.getString("007"));
		isoRequest.set("011",receivedISO.getString("011"));
		isoRequest.set("012",receivedISO.getString("012"));
		isoRequest.set("013",receivedISO.getString("013"));
		isoRequest.set("039","00");
		isoRequest.set("070","301");

		byte[] pakedBytes= isoRequest.pack();
		logger.info("HEX ISO REQUEST : "+ISOUtil.hexString(pakedBytes));
		logISOMsg(isoRequest);
		String messageString = ISOUtil.hexString(pakedBytes);
		String lengthStr =StringUtils.leftPad(Integer.toHexString(messageString.length() / 2),4, "0");
		messageString = lengthStr + messageString;

		try {
			SingleTonSocketConn.getOutputStream().write(Convert.hexStringToByteArray(messageString));
			logger.info("-------------ISO WRITTEN SUCCESSFULLY---------------");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
