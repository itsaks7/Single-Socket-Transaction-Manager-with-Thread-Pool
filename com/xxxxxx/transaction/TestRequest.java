package com.xxxxxx.transaction;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import org.jpos.iso.AsciiHexInterpreter;
import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;
import org.jpos.iso.packager.GenericPackager;

import com.xxxxxx.iso8583handler.ISO8583Handler;
import com.sun.xml.internal.ws.util.ASCIIUtility;

public class TestRequest {

	/**
	 * @param args
	 */
	private static final String OUTPUT_BYTE = "BYTE";
	private static final String OUTPUT_ASCII = "ASCII";
	private static final String OUTPUT_STRING = "STRING";
	private static final String OUTPUT_BCD = "BCD";
	private static String isoXmlPath = null;
	private static String isoDtdPath = null;
	public static void main(String[] args) throws Exception {/*
		Properties properties = new Properties();
		
		try {
			properties.load(new FileInputStream("ConnectionString.ini"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		String AtosSale="|0200|003|004000|004|012345678954|011|907539|022|1234|024|1222|025|00|037|123456789ABC|039|00|041|50000055|042|001000000034000|062|001000|";
		isoXmlPath = properties.getProperty("ISO_XML_PATH1");
		isoDtdPath = properties.getProperty("ISO_DTD_PATH1");
		ISO8583Handler isoHandler = new ISO8583Handler(isoXmlPath, isoDtdPath, "AE",
				OUTPUT_ASCII, ISO8583Handler.KEY_EXCHANGE);
		byte[] isoreq = (byte[]) isoHandler.build(AtosSale);
		// System.out.println("build msg is " + new String(isoreq));
		
		 * byte[] buildMessage = new byte[build.length()]; for (int byteCounter
		 * = 0; byteCounter < build.length(); byteCounter++) {
		 * buildMessage[byteCounter] = (byte) (build .charAt(byteCounter)); }
		 
		for (int i = 0; i < isoreq.length; i++) {
			System.out.print(String.format("%02X", isoreq[i]));

		}
		System.out.println();
		String parse = isoHandler.parse(isoreq);
		System.out.println("parse msg is " + parse);
		System.out.println("*******************");
	*/
		//sendKeyChangeRequest();
		balanceInquiry();
		///sendSMS();
		getTransactionMessage(0);
	
	}
	private static void logISOMsg(ISOMsg msg) {
		System.out.println();
		System.out.println("----ISO MESSAGE-----");
		try {
			System.out.println("    MTI : " + msg.getMTI());
			for (int i=1;i<=msg.getMaxField();i++) {
				if (msg.hasField(i)) {
					System.out.println("    Field-"+i+" : "+msg.getString(i));
				}
			}
		} catch (ISOException e) {
			e.printStackTrace();
		} finally {
			System.out.println("--------------------");
		}
 
	}
	private static void sendKeyChangeRequest(){
		try {
			Properties properties = new Properties();
			try {
				properties.load(new FileInputStream("ConnectionString.ini"));
			} catch (Exception e) {
				e.printStackTrace();
			}
			String keyChangeRequest = "|0800|003|990000|007|1115021530|011|000001|024|811|041|SBMP0001|042|SBMPOS000000001|";
			isoXmlPath = properties.getProperty("ISO_XML_PATH1");
			isoDtdPath = properties.getProperty("ISO_DTD_PATH1");
			ISO8583Handler isoHandler = new ISO8583Handler(isoXmlPath, isoDtdPath, "PEX",
					OUTPUT_ASCII, ISO8583Handler.TPK_Download);
			//String isoReq = "4856484848484948484849484848494848484848484848484848484948484848484848484848484848484848494948484848484848484848484848484848484848484848575748484848494949534850495351484848484848495649498366778048484849836677807983484848484848484849";
			//byte[] isoreq = (byte[]) isoHandler.build(keyChangeRequest);
			
				ISOMsg isoMsg = getTransactionMessage(0);
				//ISOMsg isoMsg = getNetworkMessage(2);
				//ISOMsg isoMsg = getTransactionMessage(1);
	            
			   logISOMsg(isoMsg);
	           byte[] isoreq = isoMsg.pack();
	           byte[] isoRes = null;

			Socket socket = new Socket("192.168.252.61", 8813);
			try {
				/*for(int i = 0; i < isoreq.length; i++){
					System.out.print(" "+isoreq[i]);
				}*/
				OutputStream outputStream = socket.getOutputStream();
				outputStream.write(isoreq);
				outputStream.flush();
				String parseMsg="";
				int tOut = 30;
				while (!socket.isClosed() && socket.isConnected() && !socket.isInputShutdown() && (0 < tOut--)) {
					int size = socket.getInputStream().available();
					if (size > 0) {
						byte[] isoResponse =null;
						DataInputStream bin = new DataInputStream(socket.getInputStream());
						isoResponse = new byte[size];
						bin.read(isoResponse);
						ISOMsg response = new ISOMsg();
						GenericPackager packager = new GenericPackager("iso87binary.xml");
						response.setPackager(packager);
						response.unpack(isoResponse);
						logISOMsg(response);
					}
					Thread.sleep(1000);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				if(socket!=null){
					socket.close();
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	private static ISOMsg getTransactionMessage(int i) {
		 ISOMsg isoMsg = null;
		try {
		/*	
			
    MTI : 0200
    Field-3 : 090000
    Field-4 : 000000002500
    Field-7 : 1028024220
    Field-11 : 000162
    Field-12 : 151028144219
    Field-22 : 051
    Field-24 : 983
    Field-25 : 00
    Field-35 : 5457210002001040D2512201044780762
    Field-41 : SBMP0002
    Field-42 : SBMPOS000000001
    Field-49 : 480
    Field-54 : 000000001000
    Field-55 : 9F3501229F1A0203565F2A0203569F2608925F3556A2283E368202580
    09F360200259F370498FD2033950540000480009C01099F1008010103A0000000008
    407A00000000410109F090200029F2701809F3303E0F8C89F34030200009A0315102
    89F02060000000025009F03060000000010005F3401009F1E083036303735363637*/
			if (i == 0) {
				GenericPackager packager = new GenericPackager(
						"iso87binary.xml");
				isoMsg = new ISOMsg();
				isoMsg.setPackager(packager);
				isoMsg.setMTI("0200");
				isoMsg.set(3, "090000");
				isoMsg.set(4, "000000002500");
				isoMsg.set(7, "1028024220");
				isoMsg.set(11, "000162");
				isoMsg.set(12, "151028144219");
				isoMsg.set(22, "021");
				isoMsg.set(24, "983");
				isoMsg.set(25, "00");
				isoMsg.set(35, "5457210002001040D2512201044780762");
				isoMsg.set(41, "SBMP0002");
				isoMsg.set(42, "SBMPOS000000001");
				//isoMsg.set(54, "000000001000");
				isoMsg.set(49, "480");
				
				isoMsg.set(54, "0040480C000000001000");
				/*isoMsg.set(55, "9F3501229F1A0203565F2A0203569F2608925"
						+ "F3556A2283E36820258009F360200259F370498FD20"
						+ "33950540000480009C01099F1008010103A00000000"
						+ "08407A00000000410109F090200029F2701809F3303"
						+ "E0F8C89F34030200009A031510289F0206000000002"
						+ "5009F03060000000010005F3401009F1E0830363037"
						+ "35363637");*/

				/*
				 * if(i == 1){ isoMsg.set(24, "808"); }else{ isoMsg.set(24,
				 * "811"); }
				 */
			}else if(i==1){
				Date date = new Date();
				 SimpleDateFormat dataElement7Format = new SimpleDateFormat("MMddhhmmss");
				 String dataElement7 = dataElement7Format.format(date);
				 SimpleDateFormat dataElement12Format = new SimpleDateFormat("YYMMddhhmmss");
				 String dataElement12 = dataElement12Format.format(date);
				//|0100|002|54233900074112640|003|310000|004|000000000000|007|"+dataElement7+"|011|001234|012|"+dataElement12+"|014|991231|022|021|024|100|025|00|035|54233900074112640D991233000123410000|041|SBMP0001|042|SBMPOS000000001|049|810|052|3456F43B6ED2778B
				GenericPackager packager = new GenericPackager(
						"iso87binary.xml");
				isoMsg = new ISOMsg();
				isoMsg.setPackager(packager);
				isoMsg.setMTI("0100");
				isoMsg.set(2, "5264190125349927");
				isoMsg.set(3, "310000");
				isoMsg.set(4, "000000000000");
				isoMsg.set(7, dataElement7);
				isoMsg.set(11, "000304");
				isoMsg.set(12, dataElement12);
				isoMsg.set(14, "190421");
				isoMsg.set(22, "021");
				isoMsg.set(24, "100");
				isoMsg.set(25, "00");
				isoMsg.set(35, "5264190125349927D19041010019340");
				isoMsg.set(41, "SBMP0001");
				isoMsg.set(42, "SBMPOS000000001");
				isoMsg.set(49, "480");//initially 810 changed to 480 acc to request

			}
			else if(i==2){
				//sms integration
				Date date = new Date();
				 SimpleDateFormat dataElement7Format = new SimpleDateFormat("MMddhhmmss");
				 String dataElement7 = dataElement7Format.format(date);
				 //|007|11|047|48|
				GenericPackager packager = new GenericPackager(
						"iso87binary.xml");
				isoMsg = new ISOMsg();
				isoMsg.setPackager(packager);
				isoMsg.setMTI("0303");
				isoMsg.set(7, dataElement7);
				isoMsg.set(11, "000400");
				//isoMsg.set(47, "016005Hello02400440020250049992");
				isoMsg.set(47, "016005Hello02400440020250049992");
				//isoMsg.set(48, "0360852516823");
				isoMsg.set(48, "360852516823940200");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return isoMsg;
	}
	
	private static ISOMsg getNetworkMessage(int i){
		 ISOMsg isoMsg = null;
		try {
			GenericPackager packager = new GenericPackager("iso87binary.xml");
            isoMsg = new ISOMsg();
            isoMsg.setPackager(packager);
            isoMsg.setMTI("0800");
            isoMsg.set(3, "990000");
            isoMsg.set(7, "1115021530");
            isoMsg.set(11, "000001");
            if(i == 1){
            	isoMsg.set(24, "808");
            }else{
            	isoMsg.set(24, "811");	
            }
            isoMsg.set(41, "SBMP0001");
            isoMsg.set(42, "SBMPOS000000001");
		} catch (Exception e) {
			// TODO: handle exception
		}
		return isoMsg;
	}
	
	private static void balanceInquiry(){
		try {
			Properties properties = new Properties();
			try {
				properties.load(new FileInputStream("ConnectionString.ini"));
			} catch (Exception e) {
				e.printStackTrace();
			}
			
				ISOMsg isoMsg = getNetworkMessage(2);
	            
			   logISOMsg(isoMsg);
	           byte[] isoreq = isoMsg.pack();

			Socket socket = new Socket("192.168.252.61", 8813);
			try {
				OutputStream outputStream = socket.getOutputStream();
				outputStream.write(isoreq);
				outputStream.flush();
				int tOut = 30;
				while (!socket.isClosed() && socket.isConnected() && !socket.isInputShutdown() && (0 < tOut--)) {
					System.out.println("connection is up " +tOut);
					int size = socket.getInputStream().available();
					if (size > 0) {
						byte[] isoResponse =null;
						DataInputStream bin = new DataInputStream(socket.getInputStream());
						isoResponse = new byte[size];
						bin.read(isoResponse);
						ISOMsg response = new ISOMsg();
						GenericPackager packager = new GenericPackager("iso87binary.xml");
						response.setPackager(packager);
						response.unpack(isoResponse);
						logISOMsg(response);
					}
					Thread.sleep(1000);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				if(socket!=null){
					socket.close();
				}
			}
			
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	private static void sendSMS(){
		try {
			

			Properties properties = new Properties();
			try {
				properties.load(new FileInputStream("ConnectionString.ini"));
			} catch (Exception e) {
				e.printStackTrace();
			}
			
				ISOMsg isoMsg = getTransactionMessage(2);
	            
			   logISOMsg(isoMsg);
	           byte[] isoreq = isoMsg.pack();

			Socket socket = new Socket("192.168.252.20", 8210);
			try {
				OutputStream outputStream = socket.getOutputStream();
				outputStream.write(isoreq);
				outputStream.flush();
				int tOut = 30;
				while (!socket.isClosed() && socket.isConnected() && !socket.isInputShutdown() && (0 < tOut--)) {
					System.out.println("connection is up " +tOut);
					int size = socket.getInputStream().available();
					if (size > 0) {
						byte[] isoResponse =null;
						DataInputStream bin = new DataInputStream(socket.getInputStream());
						isoResponse = new byte[size];
						bin.read(isoResponse);
						ISOMsg response = new ISOMsg();
						GenericPackager packager = new GenericPackager("iso87binary.xml");
						response.setPackager(packager);
						response.unpack(isoResponse);
						logISOMsg(response);
					}
					Thread.sleep(1000);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				if(socket!=null){
					socket.close();
				}
			}
			
			
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
