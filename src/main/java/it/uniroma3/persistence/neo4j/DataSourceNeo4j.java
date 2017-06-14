package it.uniroma3.persistence.neo4j;

import java.io.File;

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
	
	public GraphDatabaseService getDatabase(){
		this.dbFactory = new GraphDatabaseFactory();
		File storeFile = new File("/Users/leorossi/Documents/Neo4j/default.graphdb");
		this.graphDB = dbFactory.newEmbeddedDatabase(storeFile);
		return graphDB;
	}

}
