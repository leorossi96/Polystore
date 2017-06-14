package it.uniroma3.persistence.neo4j;


import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;

/**
 * Questa classe si occupa di interrogare il database Neo4j, inviando una query Cypher
 * @author micheletedesco1
 *
 */
public class GraphDao {
	private DataSourceNeo4j datasource;
	private GraphDatabaseService graphDB;
	
	public GraphDao(){
		this.datasource = new DataSourceNeo4j();
		this.graphDB = datasource.getDatabase();
	}
	
	public Result interroga(String queryCQL){
		Result result = graphDB.execute(queryCQL);
        return result;
	}
	
	public void chiudiConnessione(){
		this.graphDB.shutdown();
	}
	

}
