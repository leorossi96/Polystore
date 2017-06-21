package it.uniroma3.persistence.neo4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DataSourceNeo4j {

	private static final String USER_ID = "neo4j";
	private static final String PWD = "password";
	private static Connection ISTANCE;

	public static Connection getConnectio() {
		if (ISTANCE == null) {
			try {
				ISTANCE = DriverManager.getConnection("jdbc:neo4j:http://localhost",USER_ID,PWD);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		return ISTANCE;
	}
}
