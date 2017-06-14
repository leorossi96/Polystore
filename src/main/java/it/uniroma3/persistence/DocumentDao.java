package it.uniroma3.persistence;

import it.uniroma3.persistence.mongo.DataSourceMongo;

import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;

//da rivedere
public class DocumentDao {
	
	private DataSourceMongo datasource;
	public DocumentDao(){
		this.datasource = new DataSourceMongo();	
	}
	
	public List<String> interroga(String queryMongo) throws UnknownHostException{
		DB db = this.datasource.getDatabase();
		String tabella = this.dammiTabella(queryMongo);
		String query = this.dammiQuery(queryMongo);
		DBCollection table = db.getCollection(tabella);
		DBObject dbObject = (DBObject)JSON.parse(query);
		DBCursor cursor = table.find(dbObject);
		List<String> documenti = new LinkedList<String>();
		while (cursor.hasNext()){
			BasicDBObject oggetto = (BasicDBObject) cursor.next();
			String documento = oggetto.toString();
			documenti.add(documento);
			
		}
		return documenti;
		
		
		
	}
	
	private String dammiTabella(String queryMongo){
		String[] parti = queryMongo.split("\\.");
		String tabella = parti[1];
		return tabella;
	}
	
	private String dammiQuery(String queryMongo){
		String[] parti = queryMongo.split("\\.",3);
		String parte3 = parti[2];
		parti =  parte3.split("\\(");
		String queryNONbuona = parti[1];
		String query = queryNONbuona.substring(0,queryNONbuona.length()-1);
		return query;
	}
	
	
	

}
