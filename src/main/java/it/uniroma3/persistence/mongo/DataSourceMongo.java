package it.uniroma3.persistence.mongo;

import java.net.UnknownHostException;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class DataSourceMongo {
	
	public DB getDatabase() throws UnknownHostException{
	MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
	DB db = mongoClient.getDB( "testTirocinio" );
	return db;
	}

}
