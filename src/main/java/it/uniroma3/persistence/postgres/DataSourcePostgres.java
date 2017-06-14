package it.uniroma3.persistence.postgres;

import it.uniroma3.persistence.PersistenceException;

import java.sql.*;

/**
 * Classe che stabilisce una connesione al database postgres
 * @author micheletedesco1
 *
 */
public class DataSourcePostgres {
	
	
	private String dbURI = "jdbc:postgresql://localhost/testTir";
	private String userName = "postgres";
	private String password = "postgres";

	public Connection getConnection() throws PersistenceException {
		Connection connection = null;
		try {
		    Class.forName("org.postgresql.Driver");
		    connection = DriverManager.getConnection(dbURI,userName, password);
		} catch (ClassNotFoundException e) {
			throw new PersistenceException(e.getMessage());
		} catch(SQLException e) {
			throw new PersistenceException(e.getMessage());
		}
		return connection;
	}
}
