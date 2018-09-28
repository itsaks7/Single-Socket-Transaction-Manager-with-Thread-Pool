package com.xxxxxx.transaction;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.perf4j.log4j.Log4JStopWatch;

import com.xxxxxx.encryption.des3ede.DES3ede;
import com.xxxxxx.encryption.des3ede.Key;

/**
 * This class is responsible for taking SALE,VOID,REFUND Transaction type of
 * ISORequest and process them. modified by parag
 * 
 */
public class TransactionProcessor implements Runnable {

	private static final int RESPONSE_TIMEOUT = 30;
	private Object[] fields = null;
	private boolean inProcess = true;
	private static final String lmk = Key.getKeyfromFile("junk.ini");
	Logger logger = Logger.getLogger(TransactionProcessor.class.getName());

	HSQLConnection hSqlConnection = null;
	long transactionNumber;


	public TransactionProcessor(Object[] fields) throws Exception {
		this.fields = fields;
		inProcess = true;
	}

	@Override
	public synchronized void run() {
		if (inProcess) {
			try {

				transactionNumber = (long) Integer
						.valueOf(fields[0].toString()).intValue();

				String req = fields[1].toString();//  iso request
				fields[4].toString();
				Integer.valueOf(fields[3].toString()).intValue();

				String stringRequest = DES3ede.DD(lmk, req);// decrypted String
				// request using lmk
				new Log4JStopWatch(logger, Level.DEBUG);
				System.out.println("Request Direct >>>>>>>>"+stringRequest);
				logger.debug("Request Message : " + stringRequest);
				try {
					
					SingleTonSocketConn.getOutputStream().write(hexStringToByteArray(stringRequest));
				
					logger.debug("******************SENT SUCCESSFULLY*********************");
				} catch (Exception e) {
					logger.debug(e);
					if (SingleTonSocketConn.getInstance() != null) {
						SingleTonSocketConn.getInstance().close();

						logger.debug("SingleTonSocketConn.getInstance() *****" + SingleTonSocketConn.getInstance() == null);
						SingleTonSocketConn.setNull();

						logger.debug("***closing Client Connection");
					}

				}

				Calendar now = Calendar.getInstance();
				now.add(Calendar.SECOND, RESPONSE_TIMEOUT);
				Date tgTimeout = now.getTime();

				//new ReadResponse(transactionNumber,tgTimeout,	stringRequest, fields);


			} catch (Exception e) {
				e.printStackTrace();
			}
			finally {

				inProcess = false;

			}

		}

	}

	/*
	 * method for conversion of hexa string iso message to bytearray form
	 */
	private static byte[] hexStringToByteArray(String s) {
		int len = s.length();//529
		byte[] data = new byte[len / 2];//264
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
					+ Character.digit(s.charAt(i+1), 16));
		}
		return data;
	}
}