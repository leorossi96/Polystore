package it.uniroma3.persistence.mongo;

import java.net.UnknownHostException;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;


public class MongoDao {
	private DB db;
	

	public MongoDao() throws UnknownHostException {
		DataSourceMongo datasource = new DataSourceMongo();
		this.db = datasource.getDatabase();
		
	}

	public DBCursor interroga(DBObject query, String tabella){
		DBCollection table = db.getCollection(tabella);
		DBCursor cursor = table.find(query);
		return cursor;
	}

}
