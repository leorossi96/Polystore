package it.uniroma3.costruttoreQuery;

import java.io.StringReader;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import it.uniroma3.JsonUtils.Convertitore;
import it.uniroma3.persistence.mongo.MongoDao;

public class CostruttoreQueryMongo implements CostruttoreQuery {


	@Override
	public void eseguiQuery(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto,
			List<String> nodo, Map<String, List<List<String>>> mappaWhere, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph <String, DefaultWeightedEdge> grafoPriorita)
					throws Exception {

		Map<String , List<String>> mappaArrayFkFigli = this.getMappaArrayFkFigli(grafoPrioritaCompatto, mappaRisultati, nodo); //address.address_id ->["1","2"], customer.customer_id ->["4","9"]

		String campoSelect = this.getForeingKeyNodo(grafoPriorita,nodo.get(0),mappaWhere);
		System.out.println("CAMPO SELECT = "+campoSelect +"\n");
		StringBuilder queryRiscritta = new StringBuilder();
		if(campoSelect == null){
			campoSelect = "*";
			queryRiscritta.append("SELECT *\nFROM\n");
		}else
			queryRiscritta.append("SELECT "+campoSelect+"\nFROM\n");
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
		JsonArray risutatiFormaCorretta = this.pulisciRisultati(risultati);
		mappaRisultati.put(nodo, risutatiFormaCorretta);
		System.out.println("RISULTATO INSERITO NELLA MAPPARISULTATI: "+ risutatiFormaCorretta.toString());

	}


	/**
	 * 
	 * @return La chiave esterna sulla quale il nodo padre effettua il join. Null altrimenti.
	 */
	private String getForeingKeyNodo(SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita, String nodo, Map<String, List<List<String>>> mappaWhere) {
		String padre = null;
		for(DefaultWeightedEdge arco : grafoPriorita.incomingEdgesOf(nodo)){
			padre = grafoPriorita.getEdgeSource(arco);
		}
		if(padre != null){
			for(List<String> condizioni : mappaWhere.get(padre)){
				StringTokenizer st = new StringTokenizer(condizioni.get(1), ".");
				if(st.nextToken().equals(nodo))
					return condizioni.get(1);
			}
		}
		return null;
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

	private JsonArray pulisciRisultati(JsonArray ris) {
		StringBuilder sb = new StringBuilder();
		Iterator<JsonElement> iterator = ris.iterator();
		while (iterator.hasNext()) {
			sb.append(iterator.next().toString());
		}
		String risultati = sb.toString();
		String r = risultati.replaceAll(Pattern.quote("\\\""), "\"")
				.replaceAll(Pattern.quote("{\"value\":\"{"), "{")
				.replaceAll(Pattern.quote("}\""), "")
				.replaceAll(":([^\"].*?),", ":\"$1\",")
				.replaceAll(Pattern.quote("}{"), "},{")
				.replaceAll("([0-9]).0", "$1");
		String r2 = "["+r+"]";
		JsonReader j = new JsonReader(new StringReader(r2));
		j.setLenient(true);
		return new JsonParser().parse(j).getAsJsonArray();
	}

	private JsonArray eseguiQueryDirettamente(String query) throws Exception { 
		ResultSet risultatoResult = new MongoDao().interroga(query);
		JsonArray risultati = Convertitore.convertSQLToJson(risultatoResult);
		return risultati;
	}
}
