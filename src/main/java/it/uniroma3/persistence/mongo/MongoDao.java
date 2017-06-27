package it.uniroma3.persistence.mongo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class MongoDao {

	public ResultSet interroga(String query) {
		Connection con = DataSourceMongo.getConnectio();
		try {
			Statement statement = con.createStatement();
			ResultSet resultSet = statement.executeQuery(query);
			return resultSet;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}
}
