package it.uniroma3.persistence.postgres;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class RelationalDao {
	private DataSourcePostgres dataSource;

	public RelationalDao() {
		this.dataSource = new DataSourcePostgres();
	}

	public ResultSet interroga(String querySQL) throws SQLException {
		Connection connection = this.dataSource.getConnection();
		ResultSet result;
		Statement statement = connection.createStatement();
		result = statement.executeQuery(querySQL);

		return result;
	}

}
