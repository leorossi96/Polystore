package it.uniroma3.persistence.mongo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DataSourceMongo {

	private static final String DRIVER_NAME = "mongodb.jdbc.MongoDriver";
	private static final String DB_NAME = "Polystore2"; //testTirocinio
	private static final String URI="jdbc:mongo://localhost/"+DB_NAME+"?rebuildschema=true";
	private static final String USER_ID = "admin"; 
	private static final String PASSWORD = "password"; 
	private static Connection ISTANCE = null;

	public static Connection getConnection() throws ClassNotFoundException  {
		if(ISTANCE == null) {
			try {
				Class.forName(DRIVER_NAME);
				ISTANCE = DriverManager.getConnection(URI, USER_ID, PASSWORD);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		return ISTANCE;
	}

}
