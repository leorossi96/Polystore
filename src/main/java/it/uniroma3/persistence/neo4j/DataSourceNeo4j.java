package it.uniroma3.persistence.neo4j;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

/**
 * Classe che stabilisce una connesione al database Neo4j
 * @author micheletedesco1
 *
 */
public class DataSourceNeo4j {

	private GraphDatabaseFactory dbFactory;
	private GraphDatabaseService graphDB;
	private static final String USER_ID = "neo4j";
	private static final String PWD = "password";
	private static Connection ISTANCE;


	public GraphDatabaseService getDatabase(){
		this.dbFactory = new GraphDatabaseFactory();
		File storeFile = new File("/Users/leorossi/Documents/Neo4j/default.graphdb");
		this.graphDB = dbFactory.newEmbeddedDatabase(storeFile);
		return graphDB;
	}

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
