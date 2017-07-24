package it.uniroma3.persistence.mongo;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DataSourceMongo {
	
	private static final String properties = "mongo.properties";
	
	private static Connection ISTANCE = null;

	public static Connection getConnection() throws ClassNotFoundException, IOException  {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		Properties props = new Properties();
		InputStream resource = loader.getResourceAsStream(properties);
		props.load(resource);
		if(ISTANCE == null) {
			try {
				Class.forName(props.getProperty("DRIVER_NAME"));
				ISTANCE = DriverManager.getConnection(props.getProperty("URI"), props.getProperty("USER_ID"), props.getProperty("PASSWORD"));
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		return ISTANCE;
	}

}
