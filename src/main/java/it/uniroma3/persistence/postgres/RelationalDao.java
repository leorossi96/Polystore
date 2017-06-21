package it.uniroma3.persistence.postgres;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class RelationalDao {

	public ResultSet interroga(String querySQL) throws SQLException, ClassNotFoundException {
		Connection connection = DataSourcePostgres.getConnection();
		Statement statement = connection.createStatement();
		ResultSet result = statement.executeQuery(querySQL);
		return result;
	}
}
