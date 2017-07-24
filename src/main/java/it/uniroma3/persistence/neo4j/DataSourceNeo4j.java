package it.uniroma3.persistence.neo4j;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class DataSourceNeo4j {
	
	private static final String properties = "neo4j.properties";

	private static Connection ISTANCE;

	public static Connection getConnection() throws IOException {
		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		Properties props = new Properties();
		InputStream resource = loader.getResourceAsStream(properties);
		props.load(resource);
		if (ISTANCE == null) {
			try {
				ISTANCE = DriverManager.getConnection(props.getProperty("URI"), props.getProperty("USER_ID"), props.getProperty("PASSWORD"));
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		return ISTANCE;
	}
}
