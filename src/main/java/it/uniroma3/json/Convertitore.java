package it.uniroma3.json;

import java.sql.ResultSet;
import java.util.List;
import java.util.regex.Pattern;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Convertitore {

	public static JsonArray convertCypherToJson(ResultSet resultSet, String campoReturn, List<String> listaProiezioniNodo) throws Exception {
		JsonArray jsonArray = new JsonArray();
		JsonParser parser = new JsonParser();

		while (resultSet.next()) {
			int total_rows = resultSet.getMetaData().getColumnCount();
			JsonObject obj = null;
			for (int i = 0; i < total_rows; i++) {
				if(campoReturn != null){//è un nodo figlio e mi ritorna solo valori numerici di fk quindi va parsato e costruito il risultato in un altro modo
					obj = (JsonObject) parser.parse("{\""+campoReturn.split("\\.")[1]+"\": "+resultSet.getObject(i + 1).toString()+"}");
				}
				else{
					if(listaProiezioniNodo.isEmpty()){ //ritorno tutti i campi
						obj = (JsonObject) parser.parse(resultSet.getObject(i + 1).toString().replaceFirst("\\{([^=].*?=)\\{", "{").replaceAll("\\}, ([^=].*?=)\\{", ", ").replaceAll(Pattern.quote("}}"), "\"}").replaceAll(Pattern.quote("="), "\":\"").replaceAll(",", "\",").replaceAll("\\{", "{\"")
								.replaceAll(Pattern.quote(", "), ", \""));
					}
					else{ //è il nodo radice se mi trovo nella fase 1 dell'esecuzione o ho una selezioni dei campi da tornare se mi trovo nella fase 2 
						// prova query grande con SELECT payment.amount, payment.payment_id, rental.rental_id , rental.customer_id FROM ...
						StringBuilder rigaRisultato = new StringBuilder();
						rigaRisultato.append("{");
						for(int j = 0; j < listaProiezioniNodo.size()-1; j++){
							String campoSelectRisultato = listaProiezioniNodo.get(j).split("\\.")[1];
							rigaRisultato.append("\""+campoSelectRisultato+"\":"+resultSet.getObject((j+1)).toString()+", ");
						}
						int ultimoElemento = listaProiezioniNodo.size()-1;
						rigaRisultato.append("\""+listaProiezioniNodo.get(ultimoElemento).split("\\.")[1]+"\":"+resultSet.getObject((ultimoElemento+1)).toString()+"}");
						obj = (JsonObject) parser.parse(rigaRisultato.toString());
					}
				}
				boolean inserisci = true;
				for(JsonElement je : jsonArray)
					if(je.getAsJsonObject().equals(obj) && inserisci)
						inserisci = false;
				if(inserisci)
					jsonArray.add(obj);
			}
		}
		return jsonArray;
	}

	public static JsonArray convertSQLMongoToJson(ResultSet resultSet) throws Exception {
		JsonArray jsonArray = new JsonArray();
		while (resultSet.next()) {
			int total_rows = resultSet.getMetaData().getColumnCount();
			JsonObject obj = new JsonObject();
			for (int i = 0; i < total_rows; i++) {
				if(resultSet.getObject(i + 1) == null){
					obj.addProperty(resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase(), "");
				}
				else
					obj.addProperty(resultSet.getMetaData().getColumnLabel(i + 1).toLowerCase(), resultSet.getObject(i + 1).toString());
			}
			jsonArray.add(obj);
		}
		return jsonArray;
	}

}
