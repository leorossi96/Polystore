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
import it.uniroma3.persistence.neo4j.GraphDao;

public class CostruttoreQueryNeo4j implements CostruttoreQuery {

	@Override
	public void eseguiQuery(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto,
			List<String> nodo, Map<String, List<List<String>>> mappaWhere, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita)
					throws Exception {
		Map<String , List<String>> mappaArrayFkFigli = this.getMappaArrayFkFigli(grafoPrioritaCompatto, mappaRisultati, nodo); //address.address_id ->["1","2"], customer.customer_id ->["4","9"]

		System.out.println("MAPPA ARRAY FK FIGLI = "+mappaArrayFkFigli.toString());
		StringBuilder queryRiscritta = new StringBuilder();
		boolean isFiglio = true;
		String campoReturn = this.getForeingKeyNodo(grafoPriorita,nodo.get(0),mappaWhere);
		if(campoReturn == null){
			isFiglio = false;
		}
		String tabellaPartenza = nodo.get(0);
		queryRiscritta.append("MATCH ("+tabellaPartenza+" : "+tabellaPartenza+")\n");
		queryRiscritta.append("WHERE 1=1\n");
		for(int i=0; i<nodo.size(); i++){
			List<List<String>> condizioniTabella = mappaWhere.get(nodo.get(i));		
			for(int j=0; j<condizioniTabella.size(); j++){
				List<String> condizione = condizioniTabella.get(j);
				String primaParolaParametro = condizione.get(1).split("\\.")[0];
				if(nodo.contains(primaParolaParametro)) {//non è una condizione di join 							
					queryRiscritta.append("MATCH ("+nodo.get(i)+" : "+nodo.get(i)+")--("+primaParolaParametro+" : "+primaParolaParametro+")\n");
					queryRiscritta.append("WHERE 1=1\n");
				}
				else if(!mappaWhere.keySet().contains(primaParolaParametro))
					queryRiscritta.append("AND "+condizione.get(0)+" = "+condizione.get(1)+"\n");
				else {
					if(!this.condizioneStringente(mappaWhere, nodo.get(i), condizione.get(0)))
						queryRiscritta.append("AND "+condizione.get(0)+" IN "+mappaArrayFkFigli.get(condizione.get(1).toString())+"\n");
				}
			}
		}
		if(isFiglio)
			queryRiscritta.append("RETURN "+campoReturn);
		else {
			queryRiscritta.append("RETURN { ");
			for(int n=0; n<nodo.size()-1; n++){
				queryRiscritta.append(nodo.get(n)+" : "+nodo.get(n)+", ");
			}
			queryRiscritta.append(nodo.get(nodo.size()-1)+" : "+nodo.get(nodo.size()-1)+" }\n");
		}

		String queryNeo4j = queryRiscritta.toString();
		System.out.println("QUERY NEO4J =\n"+ queryNeo4j);
		JsonArray risultati = eseguiQueryDirettamente(queryNeo4j, campoReturn);
		JsonArray risutatiFormaCorretta = this.pulisciRisultati(risultati);
		mappaRisultati.put(nodo, risutatiFormaCorretta);		
	}

	private boolean condizioneStringente(Map<String, List<List<String>>> mappaWhere, String tabellaCorrente, String valoreCondizione) {
		boolean piùStringente = false;
		List<List<String>> condizioniTabella = mappaWhere.get(tabellaCorrente);
		for(int i=0; i<condizioniTabella.size(); i++){
			List<String> condizione = condizioniTabella.get(i);
			if(condizione.get(0).equals(valoreCondizione)){
				String primaParolaParametro = condizione.get(1).split("\\.")[0];
				if(!mappaWhere.keySet().contains(primaParolaParametro)){
					piùStringente = true;
					return piùStringente;
				}
			}
		}
		return piùStringente;
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
	 * @return 
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
		String r = risultati.replaceAll("\\{\"([^:\\{].*?)\":\\{","\\{")
				.replaceAll(Pattern.quote("}}"),"}")
				.replaceAll(Pattern.quote("}{"), "},{");
		String r2 = "["+r+"]";
		System.out.println("RISULTATI CORRETTI ="+r2); 
		JsonReader j = new JsonReader(new StringReader(r2));
		j.setLenient(true);
		return new JsonParser().parse(j).getAsJsonArray();
	}

	private JsonArray eseguiQueryDirettamente(String query, String campoReturn) throws Exception{
		ResultSet risultatoResult = new GraphDao().interroga(query);
		JsonArray risultati = Convertitore.convertCypherToJson(risultatoResult, campoReturn);
		return risultati;
	}
}
