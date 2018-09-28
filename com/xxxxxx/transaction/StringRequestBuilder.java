package com.xxxxxx.transaction;
import java.util.*;

/**
 * This class is responsible for generating ISO 0200 message enriched with
 * 'LOCAL_TRANSACTION_TIME, LOCAL_TRANSACTION_DATE, POS_ENTRY_MODE,
 * TRANSACTION_GATEWAY, POS_CONDITION_CODE, TERMIAL_ID, MERCHANT_ID's
 * 
 * @author Pradnya G. Modified by Chandrakala 07/05/2012
 * 
 */
public class StringRequestBuilder {
	static String logPrefix = "TM";
	private static final String TRACK_2_DATA_FIELD_NO = "035";
	private static final String RRN_FIELD_NO = "037";
	private static final String RESPONSE_CODE_FIELD_NO = "039";
	private static final String PROCESSING_CODE_FIELD_NO = "003";
	private static final String AUTH_CODE_FIELD_NO= "038";
	private static final String BATCH_CODE_FIELD_NO = "060";
	private static final String TERMINAL_ID_FIELD_NO = "041";
	private static final String MERCHANT_ID_FIELD_NO = "042";
	private static final String STAN_FIELD_NO = "011";
	private static final String TG_FIELD_NO = "024";
	private static final String AMOUNT_FIELD_NO = "004";
	private static final String Pan_Number="002";
	private static final String NII="024";
	private static final String EMV_DATA = "055";
	

	
	/**
	 Method to get the substring of length n from the end of a string.
	 @param   request - String from which substring to be extracted.
	 @param   subStringLength- int Desired length of the substring.
	 @return lastCharacters- String
	 @autthor Chandrakala

	*/
	public String getLastFourDigit(String request) {
		String track2Data = getFieldValue(request, TRACK_2_DATA_FIELD_NO);
		
		int endIndex = track2Data.indexOf("=");
		if (endIndex < 0) {
			endIndex = track2Data.indexOf("D");
		}
		int beginIndex = endIndex - 4;
		return (track2Data.substring(beginIndex, endIndex));
	}
	
	/**
	 * method to get the RRN from message
	 * @param request
	 * @return
	 */
	public String getRRN(String request) {
		return getFieldValue(request, RRN_FIELD_NO);
	}
	
	/**
	 * method to get the response code from message
	 * @param request
	 * @return
	 */
	public String getResponseCode(String request) {
		return getFieldValue(request, RESPONSE_CODE_FIELD_NO);
	}
	
	/**
	 * method to get the batch number from settlement request
	 * 
	 * @param request
	 * @return
	 */
	public int getBatchNumber(String request) {
		return Integer.parseInt(getFieldValue(request, BATCH_CODE_FIELD_NO));
	}
	
	
	/**
	 * method to get the amount from settlement request
	 * 
	 * @param request
	 * @return
	 */
	public String getAmount(String request) {
		return getFieldValue(request, AMOUNT_FIELD_NO);
	}


	/**
	 * method to get the processing code from message
	 * @param request
	 * @return
	 */

	public String getProcessingCode(String request){
		return getFieldValue(request, PROCESSING_CODE_FIELD_NO);
	}
	
	/**
	 * Method to get Auth Code from response message
	 * @param request
	 * @return
	 */
		public String getAuthCode(String request){
		return getFieldValue(request, AUTH_CODE_FIELD_NO);
	}
	
	
	/**
	 * GET BATCH_CODE_NUMBER FROM ISOREQUEST
	 * 
	 * @param request
	 * @return
	 */
	public String getBatchCode(String request) {
		return getFieldValue(request, BATCH_CODE_FIELD_NO);
	}

	/**
	 * GET TERMINAL_ID FROM ISOREQUEST
	 * 
	 * @param request
	 * @return
	 */
	public String getTerminalId(String request) {
		return getFieldValue(request, TERMINAL_ID_FIELD_NO);
	}

	/**
	 * Get SystemTraceAuditNo from ISOREQUEST
	 * @param request
	 * @return
	 */
	public String getSystemTraceAuditNo(String request) {
		return getFieldValue(request, STAN_FIELD_NO);
	}
	
	/**
	 * Get TgCode from ISOREQUEST
	 * @param request
	 * @return
	 */
	public String getTgCode(String request) {
		
		return getFieldValue(request, TG_FIELD_NO);
	}
	
	/**
	 * Get getMerchantId from ISOREQUEST
	 * @param request
	 * @return
	 */
	public String getMerchantId(String request) {
		
		return getFieldValue(request, MERCHANT_ID_FIELD_NO);
	}
	
	/**
	 * Returns the Field value of requested FieldNumber
	 * 
	 * @param request
	 * @param fieldNumber
	 * @return Field value
	 */
	private String getFieldValue(String request, String fieldNumber) {
		
		StringTokenizer st = new StringTokenizer(request, "|");
	
		String fieldValue = null;
		while (st.hasMoreTokens()) {
			if (st.nextToken().equalsIgnoreCase(fieldNumber)) {
				fieldValue = st.nextToken();
			}
		}
		return fieldValue;
	
	}	

	/**
	 * To insert a token in Parse Message
	 * 
	 * @param inStr
	 * @param tokNum
	 * @param tokValue
	 * @return
	 */
	static String insertToken(String inStr, int tokNum, String tokValue) {
		String outStr = "";
		int fldNum=0;
		StringTokenizer st = new StringTokenizer(inStr, "|");
		String mti = st.nextToken();

		while (st.hasMoreTokens()) {
			fldNum = Integer.parseInt(st.nextToken());
			String fldVal = st.nextToken();

			if (fldNum > tokNum) {
				outStr += String.format("|%d|%s", tokNum, tokValue) + "|"
						+ fldNum + "|" + fldVal;
				break;
			} else {
				outStr += String.format("|%d|%s", fldNum, fldVal);
			}
		}

		while (st.hasMoreTokens()) {
			outStr += String.format("|%s", st.nextToken());
		}

		if (tokNum > fldNum) {
			outStr += String.format("|%d|%s", tokNum, tokValue);
		}
		outStr += "|";
		return mti + outStr;
	}
	

	/**
	 * Method to replace field value for a given string
	 * 
	 * @param original
	 * @param find
	 * @param replacement
	 * @return
	 */
	public String replaceFieldValue(String request, String fieldNumber,
			String newValue) {
		String newRequest = "";
		StringTokenizer st = new StringTokenizer(request, "|");
		String mti=st.nextToken();
		while (st.hasMoreTokens()) {
			String token = st.nextToken();
			if (token.equalsIgnoreCase(fieldNumber)) {
				newRequest += token + "|" + newValue + "|";
				st.nextToken();
			} else {
				newRequest += token + "|" + st.nextToken() + "|";
			}
		}
		return "|"+ mti + "|" + newRequest;
	}

	public String getmti(String request) {
		StringTokenizer st = new StringTokenizer(request, "|");

		String mti = st.nextToken();
		return mti;
	}

	public String getPanNumber(String stringRequest) {
		
		return getFieldValue(stringRequest, Pan_Number);
	}

	public String getNII(String stringRequest) {
		
		return getFieldValue(stringRequest, NII);
	}

	public String getEmvData(String stringResponse) {
		// TODO Auto-generated method stub
		return getFieldValue(stringResponse, EMV_DATA);
	}

	
	
}
