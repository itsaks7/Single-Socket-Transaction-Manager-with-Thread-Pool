package com.xxxxxx.transaction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.jpos.util.Log;

import com.sun.net.httpserver.Authenticator.Success;

public class MYSQLConnection {
	Logger logger = Logger.getLogger(MYSQLConnection.class);
	
	private static final int HSQL_READ_LIMIT = 5;
	private static final  String PROCESS_STATUS = "00";
	private static final  String SALE_STATUS = "01";
	private static final  String EMV_SALE = "20";
	private static final  String EMV_PINBLOCK_SALE = "21";
	private static final  String VOID_STATUS = "02";
	private static final  String SETTLEMENT_STATUS = "03";
	private static final  String SETTLEMENT_TRAILER_STATUS = "11";
	private static final  String batchStatus = "09";
	private static final  String batchProcessStatus = "00";
	private static final  String REFUND_STATUS = "04";
	private static final  String REVERSAL_STATUS = "05";
	private static final  String PREAUTHORIZATION = "12";
	private static final  String AUTHORIZATION = "13";
	private static final  String PREAUTHORIZATION_COMPLETE = "19";
	private static final String CASH_ADVANCE = "23";
	private static final String KEY_EXCHANGE = "50";
	private static final String TMK_Download = "51";
	private static final String Key_Exchange_with_line = "52";
	private static final String Key_Exchange_without_line = "53";
	private static final String TIP = "08";
	private static final String SALE_COMPLETION = "07";
	private static final int HSQL_READ_LIMIT_NEW = 10;
	private static final String Parameter_download = "54";
	private static final  String LOG_ON = "14";
	private static final  String ECHO = "15";
	private static final  String LOG_OFF = "16";
	private static final  String CUTOVER = "18";
	private static final String TRANSACTION_TYPE_EMI_INQUIRY = "98";
	private static final String TRANSACTION_TYPE_EMI_SALE = "99";
	public static final String LOG_ON_RESPONSE = "22";
	public static final String ECHO_RESPONSE = "23";
	public static final String LOG_OFF_RESPONSE = "24";
	public static final String CUTOVER_RESPONSE = "26";
	public static final String TRANSACTION_TYPE_PROGRAM_INQUIRY="91";
	// private static final String safrequest="14";
	int count = 0;
	
	// Retrive BasicDataSource object to get DB connection
	DatabaseConnection databaseConnection = DatabaseConnection.getInstance();
	
	/**
	 * Method to get Fields of Intable to process transactions.
	 * 
	 * @return object[]
	 * @throws Exception
	 * @throws SQLException
	 */

	public synchronized Object[] readInTableRequest() {
		Object[] fields = null;

		Connection connection = null;
		PreparedStatement pstmt = null;
		ResultSet res = null;

		try {
			connection = databaseConnection.getConnection();
			pstmt = connection.prepareStatement("select * from INTABLE limit 5");
			
			res = pstmt.executeQuery();			
			fields = new Object[HSQL_READ_LIMIT];
			ResultSetMetaData rsmd = res.getMetaData();

			int numberOfColumns = rsmd.getColumnCount();
			int count = 0;

			while (res.next()) {
				Object[] columns = new Object[numberOfColumns];
				for (int i = 0; i < numberOfColumns; i++) {
					columns[i] = res.getObject(i + 1);
					if (i == 0) {
						long transactionNumber = Long.valueOf(columns[0].toString()).longValue();
						markTransactionInProcess(transactionNumber);
					}
				}

				fields[count] = columns;
				count++;
			}
		} catch (Exception e) {			
			logger.error(e.getMessage());
		} finally{
			try {
				if(res != null) {
					res.close();
				}
				if(pstmt != null) {
					pstmt.close();
				}
				if(connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
		return fields;
	}
	
	/**
	 * this method is used for update status from 01 to 00 After Fetching
	 * request from intable
	 * 
	 * @param transactionNumber
	 */
	public synchronized void markTransactionInProcess(long transactionNumber) {
		Connection connection = null;
		PreparedStatement pstmt = null;

		try {
			connection = databaseConnection.getConnection();			
			pstmt=connection.prepareStatement("update INTABLE set MESSAGE_TYPE= ? where TRANSACTION_NUMBER=?");
			pstmt.setString(1, PROCESS_STATUS);
			pstmt.setLong(2,transactionNumber);
			pstmt.executeUpdate();
		} catch (Exception e) {			
			logger.error(e.getMessage());
		} finally{
			try {				
				if(pstmt != null) {
					pstmt.close();
				}
				if(connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}

	/**
	 * Method to insert Record in OUTTABLE
	 * 
	 * @param transactionNumber
	 * @param RRN
	 * @param authCode
	 * @param responseCode
	 * @param lastFourDigit
	 * @param status
	 * @param time
	 * @throws SQLException
	 */
	public synchronized void writeOutTableResponse(Long transactionNumber, String responseCode, 
			String status, String time,int MESSAGE_TYPE, String response,String tg_name,String tid)
			throws SQLException {
		deleteFromOutTable(transactionNumber);
		Connection connection = null;
		PreparedStatement pstmt = null;
		try 
		{
			logger.info("Writing OUT Table for TxnId "+transactionNumber);
			connection = databaseConnection.getConnection();
			pstmt =connection.prepareStatement("insert into OUTTABLE(TRANSACTION_NUMBER ,"
							+ "RESPONSE_CODE, STATUS, TIME, MESSAGE_TYPE,RESPONSE,TG_NAME,TID) "
							+ "values(?,?,?,?,?,?,?,?)");
			
			pstmt.setLong(1, transactionNumber);
			pstmt.setString(2,responseCode);
			pstmt.setString(3,status);
			pstmt.setString(4,time);
			pstmt.setInt(5,MESSAGE_TYPE);
			pstmt.setString(6,response);
			pstmt.setString(7,tg_name);
			pstmt.setString(8,tid);
			pstmt.execute();
			logger.info("Writing OUT Table for TxnId "+transactionNumber+" "+"SUCCESS");
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally{
			try {
				if(pstmt != null) {
					pstmt.close();
				}
				if(connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}
	
	/**
	 * Method to fetch fields from Settlement table for batch upload
	 * transactions.
	 * 
	 * @param batch_number
	 * @param terminal_id
	 * @return
	 */
	
	public  synchronized Object[] readSettlementTableRequest() {
		Object[] fields = null;

		Connection connection = null;
		Statement stmt = null;
		ResultSet res = null;
		
		try {
			connection = databaseConnection.getConnection();
			stmt = connection.createStatement();
			res = stmt.executeQuery("select * from SETTLEMENT where STATUS = '"+ batchStatus  + "' LIMIT " + HSQL_READ_LIMIT);
			fields = new Object[HSQL_READ_LIMIT_NEW];
			ResultSetMetaData rsmd = res.getMetaData();
			int numberOfColumns = rsmd.getColumnCount();
			int count = 0;
			
			while (res.next()) {
				Object[] columns = new Object[numberOfColumns];
				for (int i = 0; i < numberOfColumns; i++) {
					columns[i] = res.getObject(i + 1);
					if (i == 0) {
						int transactionNumber = Integer.valueOf(columns[0].toString()).intValue();
						markTransactionInProcess(transactionNumber);
					}
				}
				fields[count] = columns;
				count++;				
			}
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally{
			try {
				if(res != null) {
					res.close();
				}
				if(stmt != null) {
					stmt.close();
				}
				if(connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
		return fields;
	}

	public  synchronized void markBatchUploadTransactionInProcess(int transactionNumber) {
		Connection connection = null;
		Statement stmt = null;		

		try {
			connection = databaseConnection.getConnection();
			stmt = connection.createStatement();
			stmt.execute("update SETTLEMENT set STATUS ='"+batchProcessStatus+"'where TRANSACTION_NUMBER="+transactionNumber+"");
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally {
			try {
				if(stmt != null) { 
					stmt.close();
				} 
				if(connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}

	/**
	 * this method does batch update of batch upload transactions
	 * 
	 * @param batchUpdateMap
	 */
	public synchronized void markBatchUpdateTransactionToProcess(Map<Integer, String> batchUpdateMap) {
		Connection connection = null;
		PreparedStatement pstmt = null;

		try {
			connection = databaseConnection.getConnection();
			String sql = "update SETTLEMENT set STATUS=? where TRANSACTION_NUMBER=?";
			pstmt = connection.prepareStatement(sql);

			Set<Integer> keySet = batchUpdateMap.keySet();
			Iterator<Integer> iter = keySet.iterator();

			while (iter.hasNext()) {
				int transactionNumber = iter.next();
				pstmt.setString(1, batchUpdateMap.get(transactionNumber));
				pstmt.setInt(2, transactionNumber);
				pstmt.addBatch();
			}
			pstmt.executeBatch();
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally{
			try {
				if(pstmt != null) {
					pstmt.close();
				}
				if(connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}
	
	public synchronized void deleteFromIntable(long transactionNumber) {		
		Connection connection = null;
		PreparedStatement pstmt = null;

		try {
			connection = databaseConnection.getConnection();
			pstmt = connection.prepareStatement("delete from INTABLE where TRANSACTION_NUMBER= ?");
			pstmt.setLong(1, transactionNumber);
			pstmt.execute();
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally{
			try {
				if(pstmt != null) {
					pstmt.close();
				}
				if(connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}

	public synchronized void deleteFromSAFtable(long transactionNumber) {
		Connection connection = null;
		PreparedStatement pstmt = null;

		try {			
			connection = databaseConnection.getConnection();
			logger.info("saf txn id****"+transactionNumber);
			pstmt = connection.prepareStatement("delete from SAF where TRANSACTION_NUMBER= ?");
			pstmt.setLong(1, transactionNumber);
			pstmt.execute();
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally {
			try {
				if(pstmt != null) {
					pstmt.close();
				}
				if(connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}

	public synchronized Object[] readSAFTable() {
		Object[] fields1=null;
		Connection connection = null;
		PreparedStatement pstmt = null;
		ResultSet res = null;
		
		try {
			connection = databaseConnection.getConnection();
			pstmt = connection.prepareStatement("select * from SAF where MESSAGE_TYPE= ?");
			pstmt.setString(1, REVERSAL_STATUS);
			res = pstmt.executeQuery();
			fields1 = new Object[HSQL_READ_LIMIT];
			ResultSetMetaData rsmd = res.getMetaData();
			int numberOfColumns=rsmd.getColumnCount();
			int count = 0;
			
			while (res.next()) {
				Object[] columns = new Object[numberOfColumns];
				for (int i = 0; i < numberOfColumns; i++) {
					columns[i] = res.getObject(i + 1);
					if (i == 0) {
						long transactionNumber = Integer.valueOf(columns[0].toString()).intValue();
						markTransactionInProcesssaf(transactionNumber);
					}
				}
				fields1[count] = columns;
				count++;
			}
		} catch(Exception e){
			logger.error(e.getMessage());
		} finally{
			try {
				if(res != null) {
					res.close();
				}
				if(pstmt != null) {
					pstmt.close();
				}
				if(connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
		return fields1;
	}

	public synchronized void markTransactionInProcesssaf(long transactionNumber) {
		Connection connection = null;
		PreparedStatement pstmt = null;		
		try {
			connection = databaseConnection.getConnection();
			pstmt = connection.prepareStatement("update SAF set MESSAGE_TYPE=? where TRANSACTION_NUMBER=?");
			pstmt.setString(1, PROCESS_STATUS);
			pstmt.setLong(2,transactionNumber);
			pstmt.execute();
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally{
			try {
				if(pstmt != null) {
					pstmt.close();
				}
				if(connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}

	public synchronized void updateSafTable(long transactionNumber, int counter) {
		Connection connection = null;
		PreparedStatement pstmt = null;
		try {
			connection = databaseConnection.getConnection();						
			pstmt = connection.prepareStatement("update SAF set MESSAGE_TYPE= ? , COUNTER = ? where TRANSACTION_NUMBER=?");
			pstmt.setString(1,"05");
			pstmt.setInt(2,counter);
			pstmt.setLong(3,transactionNumber);
			pstmt.execute();
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally{
			try {
				if(pstmt != null) {
					pstmt.close();
				}
				if(connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}

	public synchronized void insertRecordInLogRecord(Long TRANSACTION_NUMBER,
			int transactionType, String transactionStatus, int ISO_BUILD_END, int ISO_PARSE_END,
			int ISO_RECEIVED_IN_TIME, int WORKER_THREAD, int WRITE_OUTTABLE_END,
			String workerThreadTimestamp, String workerThreadEndTimestamp, int timeDiff) {

		Connection connection = null;
		PreparedStatement pstmt = null;

		try {
			connection = databaseConnection.getConnection();
			pstmt = connection.prepareStatement("insert into LOGRECORD(TRANSACTION_NUMBER ,TRANSACTION_TYPE,TRANSACTION_STATUS,"
							+ "ISO_BUILD_END, ISO_PARSE_END, ISO_RECEIVED_IN_TIME, WORKER_THREAD, WRITE_OUTTABLE_END,ENTRY_TIME,EXIT_TIME,TIMEDIFF) "
							+ "values(?,?,?,?,?,?,?,?,?,?,?)");
			
			pstmt.setLong(1,TRANSACTION_NUMBER);
			pstmt.setInt(2,transactionType);
			pstmt.setString(3,transactionStatus);
			pstmt.setInt(4,ISO_BUILD_END);
			pstmt.setInt(5,ISO_PARSE_END);
			pstmt.setInt(6,ISO_RECEIVED_IN_TIME);
			pstmt.setInt(7,WORKER_THREAD);
			pstmt.setInt(8,WRITE_OUTTABLE_END);
			pstmt.setString(9,workerThreadTimestamp);
			pstmt.setString(10,workerThreadEndTimestamp);
			pstmt.setInt(11,timeDiff);
			pstmt.execute();
		} catch (Exception e) {
			logger.error(e.getMessage());
		} finally{
			try {
				if(pstmt != null) {
					pstmt.close();
				}
				if(connection != null) {
					connection.close();
				}
			} catch (SQLException e) {
				logger.error(e.getMessage());
			}
		}
	}
	public void deleteFromOutTable(long transactionID) throws SQLException {
		Connection connection = null;
		PreparedStatement pstmt = null;

		try {
			connection = databaseConnection.getConnection();
			pstmt = connection.prepareStatement("delete from OUTTABLE where TRANSACTION_NUMBER = ?");
			pstmt.setLong(1, transactionID);
			logger.trace("pstmt " + pstmt);
			pstmt.execute();
		} catch (Exception e) {
			logger.error(e.getMessage());
			connection.close();
		}
		finally {
			try {
				if (pstmt != null)
					pstmt.close();
					connection.close();
			} catch (SQLException e) {
				logger.error("transactionID " + transactionID + " " + e);
			}
		}
	}
}