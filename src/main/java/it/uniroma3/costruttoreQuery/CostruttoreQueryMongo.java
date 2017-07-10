package it.uniroma3.costruttoreQuery;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import it.uniroma3.json.Convertitore;
import it.uniroma3.json.ResultCleaner;
import it.uniroma3.persistence.mongo.MongoDao;

public class CostruttoreQueryMongo extends CostruttoreQuery {

	@Override
	public void eseguiQuery(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto,
			List<String> nodo, Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph <String, DefaultWeightedEdge> grafoPriorita)
					throws Exception {

		Map<String , List<String>> mappaArrayFkFigli = this.getMappaArrayFkFigli(grafoPrioritaCompatto, mappaRisultati, nodo); //address.address_id ->["1","2"], customer.customer_id ->["4","9"]

		String campoSelect = this.getForeignKeyNodo(grafoPriorita,nodo.get(0),mappaWhere);
		System.out.println("CAMPO SELECT = "+campoSelect +"\n");
		StringBuilder queryRiscritta = new StringBuilder();
		if(campoSelect == null){ //mi trovo nella radice
			for(int i=0; i<nodo.size()-1; i++){
				queryRiscritta.append(nodo.get(i)+"."+nodo.get(i)+"_id"+", ");
			}
			queryRiscritta.append(nodo.get(nodo.size()-1)+"."+nodo.get(nodo.size()-1)+"_id"+"\nFROM\n");
//			List<String> listaProiezioniNodo = new LinkedList<>();
//			for(String tabella : nodo){
//				if(mappaSelect.get(tabella) != null)
//					listaProiezioniNodo.addAll(mappaSelect.get(tabella));
//			}
//			queryRiscritta.append("SELECT ");
//			if(listaProiezioniNodo.isEmpty())
//				queryRiscritta.append("*\nFROM\n");
//			else
//				queryRiscritta.append(listaProiezioniNodo.toString().replaceAll(Pattern.quote("["), "").replaceAll(Pattern.quote("]"), "")+"\nFROM\n");
		}else
			queryRiscritta.append("SELECT DISTINCT "+campoSelect+"\nFROM\n");

		for(int z=0; z<nodo.size(); z++){
			String tabella = nodo.get(z);
			if(z == nodo.size()-1)
				queryRiscritta.append(tabella+"\n");
			else
				queryRiscritta.append(tabella+",\n");
		}
		queryRiscritta.append("WHERE\n1=1\n");
		for (String tabella: nodo){
			List<List<String>> condizioniTabella = mappaWhere.get(tabella);
			for(int j=0; j<condizioniTabella.size(); j++){
				List<String> condizione = condizioniTabella.get(j);
				String primaParolaParametro = condizione.get(1).split("\\.")[0];
				if(nodo.contains(primaParolaParametro) || !mappaWhere.keySet().contains(primaParolaParametro)) //se la condizione è interna al nodo
					queryRiscritta.append("AND " + condizione.get(0) + " = " + condizione.get(1) +"\n");
				else{
					String valoriFkFiglio = mappaArrayFkFigli.get(condizione.get(1)).toString().replaceAll(Pattern.quote("["), "(").replaceAll(Pattern.quote("]"), ")");
					queryRiscritta.append("AND "+condizione.get(0)+" IN "+valoriFkFiglio+"\n");
				}
			}
		}

		String queryMongo = queryRiscritta.toString();
		System.out.println("QUERY MONGO : \n"+queryMongo);
		JsonArray risultati = eseguiQueryDirettamente(queryMongo);
		JsonArray risutatiFormaCorretta = ResultCleaner.fromSQLMongo(risultati);
		mappaRisultati.put(nodo, risutatiFormaCorretta);
		System.out.println("RISULTATO INSERITO NELLA MAPPARISULTATI: "+ risutatiFormaCorretta.toString());

	}
	
	@Override
	public void eseguiQueryProiezione (List<String> fkUtili, List<String> nextNodoPath, List<String> nextNextNodoPath,
			Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect,
			Map<List<String>, JsonArray> mappaRisultati) throws Exception{
		StringBuilder queryProiezione = new StringBuilder();
		queryProiezione.append("SELECT ");
		List<String> campiDaSelezionareDelNodo = new LinkedList<>();
		for(String tabella: nextNodoPath){
			if(mappaSelect.get(tabella) != null)
				campiDaSelezionareDelNodo.addAll(mappaSelect.get(tabella));
		}

		if(nextNextNodoPath != null){//vuol dire che è un nodo del path di passaggio
			String tabellaDiJoin = null;
			for(String tabella : nextNodoPath){
				for(List<String> condizioneTabella: mappaWhere.get(tabella)){
					if(condizioneTabella.get(1).split("\\.")[1].contains(nextNextNodoPath.get(0)))
						tabellaDiJoin = tabella;
				}
			}
			queryProiezione.append(nextNodoPath.get(0)+"."+nextNodoPath.get(0)+"_id, "+tabellaDiJoin+"."+nextNextNodoPath.get(0)+"_id\nFROM\n");
		}
		else{
			if(campiDaSelezionareDelNodo.get(0).equals("*")){
				queryProiezione.append("*\nFROM\n");
			}
			else{
				for(int i=0; i<campiDaSelezionareDelNodo.size()-1; i++){
					queryProiezione.append(campiDaSelezionareDelNodo.get(i)+", ");
				}
				queryProiezione.append(campiDaSelezionareDelNodo.get(campiDaSelezionareDelNodo.size()-1)+"\nFROM\n");
			}
		}


		for(int z=0; z<nextNodoPath.size()-1; z++)
			queryProiezione.append(nextNodoPath.get(z)+",\n");
		queryProiezione.append(nextNodoPath.get(nextNodoPath.size()-1)+"\nWHERE\n1=1\n");


		for (String tabella: nextNodoPath){
			List<List<String>> condizioniTabella = mappaWhere.get(tabella);
			for(int j=0; j<condizioniTabella.size(); j++){
				List<String> condizione = condizioniTabella.get(j);
				String primaParolaParametro = condizione.get(1).split("\\.")[0];
				if(nextNodoPath.contains(primaParolaParametro) || !mappaWhere.keySet().contains(primaParolaParametro)) //se la condizione è interna al nodo
					queryProiezione.append("AND " + condizione.get(0) + " = " + condizione.get(1) +"\n");
			}
		}
		if(fkUtili!=null)
			queryProiezione.append("AND "+nextNodoPath.get(0)+"."+nextNodoPath.get(0)+"_id"+" IN "+fkUtili.toString().replaceAll(Pattern.quote("["), "(").replaceAll(Pattern.quote("]"), ")")+"\n");	
		String query = queryProiezione.toString();
		System.out.println("\nQUERY PROIEZIONE MONGO =\n"+query);
		JsonArray risultati = eseguiQueryDirettamente(query);
		JsonArray risutatiFormaCorretta = ResultCleaner.fromSQLMongo(risultati);
		mappaRisultati.put(nextNodoPath, risutatiFormaCorretta);
		System.out.println("RISULTATO INSERITO NELLA MAPPARISULTATI: "+ risutatiFormaCorretta.toString());
	}

	/**
	 * creo una mappa che ha come chiave il nome della fk dei figli e come valore la lista delle fk da unsare nella funzione IN  
	 */
	private Map<String, List<String>> getMappaArrayFkFigli(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto, Map<List<String>, JsonArray> mappaRisultati, List<String> nodo){
		Map<String , List<String>> mappaArrayFkFigli = new HashMap<>(); //address.address_id ->["1","2"], customer.customer_id ->["4","9"]
		for(DefaultWeightedEdge arco : grafoPrioritaCompatto.outgoingEdgesOf(nodo)){
			String tabellaFiglio = grafoPrioritaCompatto.getEdgeTarget(arco).get(0);
			JsonArray risFiglio = mappaRisultati.get(grafoPrioritaCompatto.getEdgeTarget(arco));
			for(JsonElement e: risFiglio){
				JsonObject jo = e.getAsJsonObject();
				String campoFk = e.toString().split("\":")[0].replaceAll(Pattern.quote("{\""), "");
				if(!mappaArrayFkFigli.containsKey(tabellaFiglio+"."+campoFk)){
					List<String> temp = new LinkedList<String>();
					temp.add((jo.get(campoFk).getAsString()));
					mappaArrayFkFigli.put(tabellaFiglio+"."+campoFk, temp);
				}
				else
					mappaArrayFkFigli.get(tabellaFiglio+"."+campoFk).add(jo.get(campoFk).getAsString());
			}
		}
		return mappaArrayFkFigli;
	}

	private JsonArray eseguiQueryDirettamente(String query) throws Exception { 
		ResultSet risultatoResult = new MongoDao().interroga(query);
		JsonArray risultati = Convertitore.convertSQLMongoToJson(risultatoResult);
		return risultati;
	}
}
