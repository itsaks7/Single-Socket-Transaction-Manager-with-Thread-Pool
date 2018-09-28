package com.xxxxxx.transaction;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;
import org.jpos.iso.packager.GenericPackager;
import org.jpos.util.Log;

public class ReadInputStream extends Thread{
	
	/**
	This Class does reading InputStream response from switch
	 */
	public ReadInputStream(Socket socket) {
		this.socket =socket;
	}
	Logger logger = Logger.getLogger(ReadInputStream.class.getName());
	private static StringRequestBuilder requestBuilder;
	private static  String STATUS_RESPONSE_SUCCESS = "00";
	private static  String STATUS_RESPONSE_SUCCESS_3DIGIT = "000";
	private static final String STATUS_RESPONSE_TIMEOUT = "97";
	private static final String STATUS_RESPONSE_ERROR = "09";
	public static Socket socket;
	String parseMsg="";
	byte[] isoResponse =null;
	HSQLConnection hSqlConnection=null;
		@Override
		public void run() {
			logger.debug("********************inside Read Thread");
			//Initialization of Connection.ini file
		
			try {
				logger.debug("Reading Connection.ini file");
				Properties properties = new Properties();
		    	FileInputStream in;
				in = new FileInputStream("ConnectionString.ini");
				properties.load(in);
			} catch (FileNotFoundException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	
			
	while(true){
		try {
			byte[] pendingStream=null;
			DataInputStream bin=null;
		socket = SingleTonSocketConn.getInstance();
		while(!socket.isClosed() && socket.isConnected() && !socket.isInputShutdown()) {
		int size = socket.getInputStream().available();
					bin = new DataInputStream(socket.getInputStream());
					isoResponse = new byte[size];
							//bin.read(isoResponse);
						 
		
		if(bin.available() > 0){
			requestBuilder = new StringRequestBuilder();
			logger.debug("Read Data :: "+bin.toString());
			pendingStream = parseRequestMessage(bin);
			
		}
		//pendingStream
		}
		}catch (IOException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
			
	}
	
}
		

	private byte[] parseRequestMessage(DataInputStream bin) {
		logger.debug("Parsing Message.");
		ISOMsg response = new ISOMsg();
		String messageString="";
		GenericPackager packager;
		boolean end = false;
		
		try {
			
			int bytesRead = 0;
			/*String abc ="";
			for(int i = 0; i >= isoResponse.length; i++){
				System.out.print(isoResponse[i]);
			}*/
		   /* isoResponse[0] = bin.readByte();
		    isoResponse[1] = bin.readByte();
		    logger.debug("Parsing Length ::"+isoResponse[0]+"-----Length :: "+isoResponse[1]);
		    int bytesToRead = isoResponse[1];*/
		    	//parsing Data
		    while(!end)
		    {
		    	logger.debug("inside END while");
		        bytesRead =bin.read(isoResponse);
		        messageString += new String(isoResponse, 0, isoResponse.length);
		        logger.debug("Message String :: "+messageString);
		        logger.debug("Message String Length :: "+messageString.length());
		   //     if (messageString.length() > bytesToRead )
		     //   {
		        	logger.debug("END true......");
		            end = true;
		  //      }
		    }
		    logger.debug("MESSAGE: " + messageString);
			
		packager = new GenericPackager("iso87binary.xml");
		byte[] iso = messageString.getBytes();
		response.setPackager(packager);
	
			response.unpack(iso);
	
		parseMsg = logISOMsg(response);
		logger.debug("Response :: "+parseMsg);
		
		Date transactionTime = new Date(System.currentTimeMillis());
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		String transactionTimestamp = sdf.format(transactionTime);
		String response_terminal_id=requestBuilder.getTerminalId(parseMsg);//response.getString(41);
		String responseCode =requestBuilder.getResponseCode(parseMsg);// response.getString(39);
		String responseStatus="";
		if("00".equals(responseCode)){
			responseStatus=STATUS_RESPONSE_SUCCESS;
		}
		int MESSAGE_TYPE=1;
		logger.debug("WRITE OUTTABLE START");
		hSqlConnection = new HSQLConnection();
		hSqlConnection.writeOutTableResponse(Long.valueOf(0),
				responseCode,responseStatus,
				transactionTimestamp, MESSAGE_TYPE, parseMsg ,"VISTA",response_terminal_id);
		
		}catch(Exception e){
			e.printStackTrace();
		}
		return null;
		
	}
private String logISOMsg(ISOMsg msg) {
	String isoResponse = "";
	
	logger.info("----ISO MESSAGE-----");
	try {
		System.out.println("    MTI : " + msg.getMTI());
		isoResponse =  "|"+msg.getMTI();
		for (int i=1;i<=msg.getMaxField();i++) {
			if (msg.hasField(i)) {
				logger.info("    Field-"+i+" : "+msg.getString(i));
				String bitNum = String.format("%03d", i);
				isoResponse += "|" + bitNum + "|" + msg.getString(i);
			}
		}
	} catch (ISOException e) {
		e.printStackTrace();
	} finally {
		logger.info("--------------------");
	}
	return isoResponse + "|";
}

}
