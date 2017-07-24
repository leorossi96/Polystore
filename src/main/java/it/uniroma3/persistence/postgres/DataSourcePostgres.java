package it.uniroma3.persistence.postgres;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class DataSourcePostgres {
	private static final String properties = "postgresql.properties";

	private static Connection ISTANCE = null;

	public static Connection getConnection() throws ClassNotFoundException, SQLException {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		Properties props = new Properties();
		InputStream resource = loader.getResourceAsStream(properties);
		try {
			props.load(resource);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if(ISTANCE == null) {
			Class.forName(props.getProperty("DRIVER"));
			ISTANCE = DriverManager.getConnection(props.getProperty("URI"), props.getProperty("USER_ID"), props.getProperty("PASSWORD"));
		}
		return ISTANCE;
	}
}
