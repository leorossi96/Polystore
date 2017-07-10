package it.uniroma3.persistence.postgres;

import java.sql.*;

public class DataSourcePostgres {

	private static final String DRIVER = "org.postgresql.Driver";
	private static final String DATABASE = "testTir";
	private static final String URI = "jdbc:postgresql://localhost/"+DATABASE;
	private static final String USER_ID = "postgres";
	private static final String PASSWORD = "postgres";
	private static Connection ISTANCE = null;

	public static Connection getConnection() throws ClassNotFoundException, SQLException {
		if(ISTANCE == null) {
			Class.forName(DRIVER);
			ISTANCE = DriverManager.getConnection(URI,USER_ID, PASSWORD);
		}
		return ISTANCE;
	}
}
