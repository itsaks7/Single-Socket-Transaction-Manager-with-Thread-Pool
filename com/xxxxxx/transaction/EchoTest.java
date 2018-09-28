package com.xxxxxx.transaction;

import java.io.IOException;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;
import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;
import org.jpos.iso.ISOUtil;
import org.jpos.iso.packager.GenericPackager;
//import com.xxxxxx.iso93.*;
import org.jpos.util.Log;

/**
 * This class is responsible for taking SALE,VOID,REFUND Transaction type of
 * ISORequest and process them. modified by parag
 * 
 */
public class EchoTest implements Runnable {

	boolean isTimedOut = false;	
	public static final int ECHO_REQUEST = 15;
	public static final int ECHO_RESPONSE = 23;
	public static Socket socket;
	public int echo_stan =0;
	public int TimeoutCounter=0;
	Logger logger = Logger.getLogger(EchoTest.class.getName());
	public static final String ECHO_MESSAGE="ECHO_MESSAGE";
	public static final String SIGN_ON_MESSAGE="SIGN_ON_MESSAGE";
	public static final String KEY_EXC_MESSAGE="KEY_EXC_MESSAGE";
	public static final int THREAD_SLEEP=1000;

	MYSQLConnection hSqlConnection=null;

	public EchoTest() throws Exception {
		logger.info("-----------ECHO SERVICE STARTED-----------");
		run();
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

	public synchronized void run() {
		
		try{
		socket = SingleTonSocketConn.getInstance();
		}
		catch(Exception ex)
		{
			logger.info("Exception Occured "+ex);
			
		}
		
		while(true) {
			if(echo_stan==999999)
				echo_stan=0;
			System.out.println("Started");
			echo_stan++;
			try {

				byte[] buildMessage= buildISOMessage(ECHO_MESSAGE);
				String messageString = ISOUtil.hexString(buildMessage);
				String lengthStr =StringUtils.leftPad(Integer.toHexString(messageString.length() / 2),4, "0");
				messageString = lengthStr + messageString;
				Thread.sleep(THREAD_SLEEP);
			}catch(Exception e){
					logger.debug("----Other Exeption from ------ "+getClass());
					e.printStackTrace();
					if(socket!=null){
						try {
							socket.close();
						} catch (IOException e1) {
							// TODO Auto-generated catch block
							e1.printStackTrace();
						}
						logger.debug("socket *****"+ socket==null);
						socket=null;
						logger.debug("***closing socket");
					}
					socket = SingleTonSocketConn.getInstance();
				}

			if (isTimedOut){
				TimeoutCounter++;
				System.out.println("Time out counter :: "+TimeoutCounter);
				if(TimeoutCounter==2){
					TimeoutCounter=0;
					try {

						socket.close();
						socket=null;
						System.out.println("socket *****"+ socket==null);
						socket = SingleTonSocketConn.getInstance();
						run();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}else{
					run();
				}
			}
		}
	}
	private  byte[] buildISOMessage(String messageType) throws ISOException
	{
		GenericPackager packager = new GenericPackager(Configuration.getValue(Configuration.ISO_BINARY_PATH));
		ISOMsg isoRequest = new ISOMsg();
		isoRequest.setPackager(packager);
		String stan=String.valueOf(echo_stan);

		Date date = new Date();
		SimpleDateFormat mmdd=new SimpleDateFormat("MMdd");
		String de13Time=mmdd.format(date);
		SimpleDateFormat MMddhhmmss=new SimpleDateFormat("MMddHHmmss");
		String de7Time=MMddhhmmss.format(date);
		SimpleDateFormat hhmmss=new SimpleDateFormat("HHmmss");
		String de12=hhmmss.format(date);

		isoRequest.setMTI("0800");
		isoRequest.set(011,stan);
		isoRequest.set(007,de7Time);
		isoRequest.set(012,de12);
		isoRequest.set(013,de13Time);
		switch(messageType)
		{
		case SIGN_ON_MESSAGE:
			isoRequest.set(070,"001");
			break;
		case KEY_EXC_MESSAGE:
			isoRequest.set(070,"101");
			break;
		case ECHO_MESSAGE:
			isoRequest.set(070,"301");
			break;	
		}
		return isoRequest.pack();
	}
	public String convertStringToHex(String str) {

		char[] chars = str.toCharArray();

		StringBuffer hex = new StringBuffer();
		for (int i = 0; i < chars.length; i++) {
			hex.append(Integer.toHexString((int) chars[i]));
		}

		return hex.toString();
	}
}