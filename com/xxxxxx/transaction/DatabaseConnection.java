package com.xxxxxx.transaction;

import java.sql.Connection;
import java.util.Properties;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.Logger;

public class DatabaseConnection {
	Logger logger = Logger.getLogger(MYSQLConnection.class);

	private static DatabaseConnection databaseConnection = null;
    private BasicDataSource basicDataSource;

    private DatabaseConnection() {
    	try {
    			logger.info("Started loading DB properties");
    			// Load connection properties from congiguration files.
	    		Properties properties = new Properties();
	    	    properties.load(new java.io.FileInputStream("ConnectionString.ini"));

	    	    logger.info("DB properties loaded successfully!");
	    	    // Create instance of BasicDataSource
	    	    basicDataSource = new BasicDataSource();

	    	    // Setting of DBCP connection pooling, DBCP can work with defaults setting also
	    	    basicDataSource.setDriverClassName(properties.getProperty("MYSQL_DRIVER_CLASS"));
	    	    basicDataSource.setUrl(properties.getProperty("MYSQL_JDBC_URL"));
	    	    basicDataSource.setUsername(properties.getProperty("MYSQL_USER_NAME"));
	    	    basicDataSource.setPassword(properties.getProperty("MYSQL_PASSWORD"));
	    	    basicDataSource.setMaxIdle(Integer.parseInt(properties.getProperty("MAX_IDLE_TIME")));
	    	    basicDataSource.setMaxWaitMillis(Integer.parseInt(properties.getProperty("MAX_WAIT_MILLIS")));
	    	    basicDataSource.setValidationQuery(properties.getProperty("VALIDATION_QUERY"));
	    	    basicDataSource.setValidationQueryTimeout(Integer.parseInt(properties.getProperty("VALIDATION_QUERY_TIMEOUT")));
	    	    basicDataSource.setTimeBetweenEvictionRunsMillis(Integer.parseInt(properties.getProperty("TIME_BETWEEN_EVICTION_RUNS_MILLIS")));
	    	    basicDataSource.setInitialSize(Integer.parseInt(properties.getProperty("INITIAL_SIZE")));
	    	    basicDataSource.setRemoveAbandonedTimeout(Integer.parseInt(properties.getProperty("REMOVE_ABANDONED_TIMEOUT")));
	    	    basicDataSource.setRemoveAbandonedOnMaintenance(Boolean.parseBoolean(properties.getProperty("REMOVE_ABANDONED_ON_MAINTENANCE")));
	    	    basicDataSource.setLogAbandoned(Boolean.parseBoolean(properties.getProperty("LOG_ABANDONED")));
	    	    basicDataSource.setMinEvictableIdleTimeMillis(Integer.parseInt(properties.getProperty("MIN_EVICTABLE_IDLE_TIME_MILLIS")));

	    	    logger.info("BasicDataSource object created successfully!");
    	    } catch(Exception e) {
    	    	logger.error("Error occured while BasicDataSource initilization: "+e.getMessage());
    		}
    }

    /* Allow only one instance of this class so that always only one connection pool created.
     * If we allow multiple instance then all TM's thread will create seperate instance of this class.
     * and in this case each instance will create a JDBC pool with specified configuration and then
     * MySql reach to maximum no of connection limits.     * 
     *     
     * There will be one case in which DatabaseConnection instance is null and at that time if two or more thread are trying to create  
     * instance then again multiple instances are created so we need to make it synchronized.
    */

    public static synchronized DatabaseConnection getInstance() {
        if (databaseConnection == null) {
        	databaseConnection = new DatabaseConnection();
            return databaseConnection;
        } else {
            return databaseConnection;
        }
    }

    // retrive connection from pool
    public Connection getConnection() {
    	Connection connection = null;
    	try {
			connection = this.basicDataSource.getConnection();
		} catch (Exception e) {
			logger.error("Unable to get connection from POOL: "+e.getMessage());
		}
        return connection;
    }    
}