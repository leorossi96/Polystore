package it.uniroma3.json;

import java.sql.ResultSet;
import java.util.List;

import com.google.gson.JsonArray;
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
					System.out.println("PROVA CHIAVI FIGLIO = "+"{\""+campoReturn.split("\\.")[1]+"\": "+resultSet.getObject(i + 1).toString()+"}");
					obj = (JsonObject) parser.parse("{\""+campoReturn.split("\\.")[1]+"\": "+resultSet.getObject(i + 1).toString()+"}");
				}
				else{
					if(listaProiezioniNodo.isEmpty()){ //ritorno tutti i campi
						System.out.println("PROVA NO FIGLIO RITORNO TUTTI I CAMPI = "+parser.parse(resultSet.getObject(i + 1).toString()));
						obj = (JsonObject) parser.parse(resultSet.getObject(i + 1).toString());
					}
					else{ //è il nodo radice e ho una selezioni dei campi da tornare 
						  //PROBLEMA!!! IN QUESTO CASO AGGIUNGE LA STESSA RIGA DI RISULTATO N VOLTE CON N = numero di campi selezionati per quel nodo
						  // prova query grande con SELECT ayment.amount, payment.payment_id, rental.rental_id , rental.customer_id FROM ...
						StringBuilder rigaRisultato = new StringBuilder();
						rigaRisultato.append("{");
						for(int j = 0; j < listaProiezioniNodo.size()-1; j++){
							String campoSelectRisultato = listaProiezioniNodo.get(j).split("\\.")[1];
							rigaRisultato.append("\""+campoSelectRisultato+"\":"+resultSet.getObject((j+1)).toString()+", ");
						}
						int ultimoElemento = listaProiezioniNodo.size()-1;
						rigaRisultato.append("\""+listaProiezioniNodo.get(ultimoElemento).split("\\.")[1]+"\":"+resultSet.getObject((ultimoElemento+1)).toString()+"}");
						System.out.println("PROVA RIGA RISULTATO CON SELECT = "+rigaRisultato.toString());
						obj = (JsonObject) parser.parse(rigaRisultato.toString());


					}
				}
					jsonArray.add(obj);
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

}
