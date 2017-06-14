package it.uniroma3.persistence.postgres;

import it.uniroma3.persistence.PersistenceException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Questa classe si occupa di interrogare il database postgres, inviando una query SQL
 * @author micheletedesco1
 *
 */
public class RelationalDao {
	private DataSourcePostgres dataSource;

	public RelationalDao() {
		this.dataSource = new DataSourcePostgres();
	}

	public ResultSet interroga(String querySQL) {
		Connection connection = this.dataSource.getConnection();
		ResultSet result;
		try {
			PreparedStatement statement;
			statement = connection.prepareStatement(querySQL);
			result = statement.executeQuery();
		} catch (SQLException e) {
			throw new PersistenceException(e.getMessage());
		} finally {
			try {
				connection.close();
			} catch (SQLException e) {
				throw new PersistenceException(e.getMessage());
			}
		}	
		return result;
	}

}
