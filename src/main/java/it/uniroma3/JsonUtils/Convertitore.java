package it.uniroma3.JsonUtils;

import java.sql.ResultSet;


import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Convertitore {

	public static JsonArray convertCypherToJson(ResultSet resultSet) throws Exception {
		JsonArray jsonArray = new JsonArray();
		JsonParser parser = new JsonParser();

		while (resultSet.next()) {
			int total_rows = resultSet.getMetaData().getColumnCount();
			JsonObject obj = null;
			for (int i = 0; i < total_rows; i++) {
				obj = (JsonObject) parser.parse(resultSet.getObject(i + 1).toString());
				jsonArray.add(obj);
				//				obj.addProperty(resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase(), resultSet.getObject(i + 1).toString());
			}
		}
		return jsonArray;
	}
	
	public static JsonArray convertSQLToJson(ResultSet resultSet) throws Exception {
		JsonArray jsonArray = new JsonArray();
		while (resultSet.next()) {
			int total_rows = resultSet.getMetaData().getColumnCount();
			JsonObject obj = new JsonObject();
			for (int i = 0; i < total_rows; i++) {
				obj.addProperty(resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase(), resultSet.getObject(i + 1).toString());
			}
			jsonArray.add(obj);
		}
		return jsonArray;
	}

	//	/**
	//	 * Converte il Result della query Cypher in jsonArray
	//	 */
	//	public static JsonArray convertCypherToJSON(Result result) throws Exception {
	//		JsonArray jsonArray = new JsonArray();
	//		while ( result.hasNext() ) {
	//			Map<String, Object> row = result.next();
	//			System.out.println("\n\nROW = "+row.toString()+"\n\n");
	//			JsonObject obj = new JsonObject();
	//			for ( String key : result.columns()){
	//				System.out.println("KEY = "+key);
	//				System.out.println("row.get(key).toString() = "+ row.get(key).toString());
	//				obj.addProperty(key,row.get(key).toString());
	//			}
	//			jsonArray.add(obj);
	//
	//		}
	//		return jsonArray;
	//	}
	////	
	////	public static JsonArray convertCypherToJSON(Result result) throws Exception {
	////		JsonArray jsonArray = new JsonArray();
	////		while ( result.hasNext() ) {
	////
	////
	////			Map<String, Object> row = result.next();
	////			JsonObject obj = new JsonObject();
	////			StringBuilder ennupla = new StringBuilder();
	////			for ( String key : result.columns() ) {
	////				Iterator i = result.columnAs(key);
	////				while (i.hasNext())	{
	////					ennupla.append(i.toString());
	////				}
	////				obj.addProperty(key,ennupla.toString());
	////			}
	////			jsonArray.add(obj);
	////		}
	////		return jsonArray;
	////	}

}

