package it.uniroma3.grafiPriotita;

import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;

import com.google.gson.JsonObject;

public class FabbricatoreMappaStatement {

	public Map<String, List<List<String>>> creaMappaWhere(List<List<String>> matriceWhere, Map<String, JsonObject> jsonCheMiServono ) throws UnknownHostException, FileNotFoundException, JSQLParserException {

		Map<String, List<List<String>>> mappaWhere = new HashMap<>();

		Set<String> tabelle = jsonCheMiServono.keySet();
		
		for(String tabella:tabelle){
			mappaWhere.put(tabella, new LinkedList<List<String>>());
		}


		for(List<String> rigaMatrice: matriceWhere){
			String tabellaPrimaCondizione = rigaMatrice.get(0).split("\\.")[0];
			String tabellaSecondaCondizione = rigaMatrice.get(1).split("\\.")[0];
			if(tabellaSecondaCondizione != null && tabelle.contains(tabellaSecondaCondizione)){
				if(!tabellaPrimaCondizione.equals(rigaMatrice.get(0).split("\\.")[1].split("_")[0]))
					mappaWhere.get(tabellaPrimaCondizione).add(rigaMatrice);
				else{
					List<String> rigaMatriceInvertita = new LinkedList<>();
					rigaMatriceInvertita.add(rigaMatrice.get(1));
					rigaMatriceInvertita.add(rigaMatrice.get(0));
					mappaWhere.get(tabellaSecondaCondizione).add(rigaMatriceInvertita);
				}
			}
			else{ // aggiungilo a tabella nella mappaWhere
				mappaWhere.get(tabellaPrimaCondizione).add(rigaMatrice);
			}
		}
		return mappaWhere;
	}

	public Map<String, List<String>> creaMappaSelect(List<String> listaProiezioni, Map<String, JsonObject> jsonUtili) {
		Map<String, List<String>> mappaSelect = new HashMap<>();
		if(listaProiezioni.isEmpty() || listaProiezioni.get(0).equals("*")){
			Iterator<String> i = jsonUtili.keySet().iterator();
			while(i.hasNext()){
				List<String> noProiezioni = new LinkedList<>();
				noProiezioni.add("*");
				mappaSelect.put(i.next(), noProiezioni);
			}
		}
		
		for(String proiezione : listaProiezioni){
			String tabellaProiezione = proiezione.split("\\.")[0];
			if(mappaSelect.containsKey(tabellaProiezione))
				mappaSelect.get(tabellaProiezione).add(proiezione);
			else{
				List<String> proiezioniTabella = new LinkedList<>();
				proiezioniTabella.add(proiezione);
				mappaSelect.put(tabellaProiezione, proiezioniTabella);
			}
		}
		return mappaSelect;
	}
	
	public Map<String, List<String>> getMappaDB(Map<String, JsonObject> jsonUtili){
		Map<String, List<String>> mappaDB = new HashMap<>();
		for(String k : jsonUtili.keySet()){
			String database = jsonUtili.get(k).get("database").getAsString();
			if(!mappaDB.containsKey(database))
				mappaDB.put(database, new LinkedList<String>());
			mappaDB.get(database).add(jsonUtili.get(k).get("table").getAsString());
		}
		return mappaDB;
	}
}
