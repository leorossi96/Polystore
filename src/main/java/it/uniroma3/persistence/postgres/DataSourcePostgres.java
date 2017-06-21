package it.uniroma3.persistence.postgres;

import java.sql.*;

public class DataSourcePostgres {

	private static final String URI = "jdbc:postgresql://localhost/testTir";
	private static final String USER_ID = "postgres";
	private static final String PWD = "postgres";
	private static Connection ISTANCE = null;

	public static Connection getConnection() throws ClassNotFoundException, SQLException {
		if(ISTANCE == null) {
			Class.forName("org.postgresql.Driver");
			ISTANCE = DriverManager.getConnection(URI,USER_ID, PWD);
		}
		return ISTANCE;
	}
}
