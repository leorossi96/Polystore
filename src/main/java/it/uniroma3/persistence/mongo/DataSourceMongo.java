package it.uniroma3.persistence.mongo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DataSourceMongo {

	private static final String DRIVER_NAME = "mongodb.jdbc.MongoDriver";
	private static final String DB_NAME = "testTirocinio";
	private static final String URL="jdbc:mongo://localhost/"+DB_NAME+"?rebuildschema=true";
	private static final String USER_ID = "admin"; 
	private static final String PWD = "password"; 
	private static Connection ISTANCE = null;

	public static Connection getConnectio() throws ClassNotFoundException  {
		if(ISTANCE == null) {
			try {
				Class.forName(DRIVER_NAME);
				ISTANCE = DriverManager.getConnection(URL, USER_ID, PWD);
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
		return ISTANCE;
	}

}
