package it.uniroma3.JsonUtils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import it.uniroma3.costruttoreQuery.CostruttoreQuery;
import it.uniroma3.costruttoreQuery.CostruttoreQueryMongo;
import it.uniroma3.costruttoreQuery.CostruttoreQueryNeo4j;
import it.uniroma3.costruttoreQuery.CostruttoreQuerySQL;


public class GestoreQuery {

	private Map<List<String>, JsonArray> mappaRisultati = new HashMap<>();

	public void esegui(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto, SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoCopia, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita, Map<String, JsonObject> jsonUtili, Map<String, List<List<String>>> mappaWhere) throws Exception{
		List<List<String>> foglie = this.getFoglie(grafoCopia);
		if(foglie.size() == 0) //radice
			return;
		else{
			Iterator<List<String>> i = foglie.iterator();
			while(i.hasNext()){
				List<String> foglia = i.next();
				eseguiQuery(grafoPrioritaCompatto, foglia, jsonUtili, mappaWhere, this.mappaRisultati, grafoPriorita);
				//aggiornaWeights(grafoPrioritaCompatto, mappaWeights, mappaRisultati, foglia) devo avere coma parametro di esegui mappaWeights
				//				grafoCopia.removeAllEdges(grafoCopia.incomingEdgesOf(foglia));
				grafoCopia.removeVertex(foglia);
			}
		}
		System.out.println("MAPPA RISULTATI = "+ this.mappaRisultati.toString());
		//		mappaRisultati.toString().replaceAll(Pattern.quote("{\"store\":\"{"), "{");
		//		System.out.println("MAPPA RISULTATI = "+ mappaRisultati.toString());
		esegui(grafoPrioritaCompatto, grafoCopia, grafoPriorita, jsonUtili, mappaWhere);
	}

	private void eseguiQuery(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto, List<String> nodo, Map<String, JsonObject> jsonUtili,
			Map<String, List<List<String>>> mappaWhere, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita) throws Exception {
		CostruttoreQuery costruttoreQuery = null;
		JsonObject myJson = jsonUtili.get(nodo.get(0)).getAsJsonObject();
		if(myJson.get("database").getAsString().equalsIgnoreCase("postgreSQL"))
			costruttoreQuery = new CostruttoreQuerySQL();
		if(myJson.get("database").getAsString().equalsIgnoreCase("mongoDB"))
			costruttoreQuery = new CostruttoreQueryMongo();
		if(myJson.get("database").getAsString().equalsIgnoreCase("neo4j"))
			costruttoreQuery = new CostruttoreQueryNeo4j();
		costruttoreQuery.eseguiQuery(grafoPrioritaCompatto, nodo, mappaWhere, mappaRisultati, grafoPriorita);

	}

	public List<List<String>> getFoglie(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> g){
		List<List<String>> foglie = new LinkedList<>();
		for(List<String> vertex: g.vertexSet())
			if(g.outgoingEdgesOf(vertex).size() == 0)
				foglie.add(vertex);
		return foglie;
	}

}
