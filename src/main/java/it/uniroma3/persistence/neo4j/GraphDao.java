package it.uniroma3.persistence.neo4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class GraphDao {

	public ResultSet interroga(String query) throws SQLException, IOException{
		Connection con = DataSourceNeo4j.getConnection();
		Statement stmt = con.createStatement();
		ResultSet results = stmt.executeQuery(query);
		return results;
	}
}
