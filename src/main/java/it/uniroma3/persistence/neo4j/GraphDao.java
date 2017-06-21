package it.uniroma3.persistence.neo4j;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Questa classe si occupa di interrogare il database Neo4j, inviando una query Cypher
 *
 */
public class GraphDao {
//	private DataSourceNeo4j datasource;
//	private GraphDatabaseService graphDB;
	
//	public GraphDao(){
//		this.datasource = new DataSourceNeo4j();
//		this.graphDB = datasource.getDatabase();
//	}
//	
//	public Result interroga(String queryCQL){
//		Result result = graphDB.execute(queryCQL);
//        return result;
//	}
	
//	public void chiudiConnessione(){
//		this.graphDB.shutdown();
//	}
	
	public ResultSet interroga(String query) throws SQLException{
		Connection con = DataSourceNeo4j.getConnectio();
		 Statement stmt = con.createStatement();
			ResultSet results = stmt.executeQuery(query);
			return results;

	}
	
}
