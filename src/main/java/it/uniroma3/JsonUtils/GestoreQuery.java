package it.uniroma3.JsonUtils;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

import it.uniroma3.costruttoreQuery.CostruttoreQuery;
import it.uniroma3.costruttoreQuery.CostruttoreQueryMongo;
import it.uniroma3.costruttoreQuery.CostruttoreQueryNeo4j;
import it.uniroma3.costruttoreQuery.CostruttoreQuerySQL;


public class GestoreQuery {

	public void esegui(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto, SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoCopia, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita, Map<String, JsonObject> jsonUtili, Map<String, List<List<String>>> mappaWhere, Map<List<String>, JsonArray> mappaRisultati) throws Exception{
		List<List<String>> foglie = getFoglie(grafoCopia);
		if(foglie.size() == 0) //sono arrivato alla radice
			return;
		else{
			Iterator<List<String>> i = foglie.iterator();
			while(i.hasNext()){
				List<String> foglia = i.next();
				eseguiQuery(grafoPrioritaCompatto, foglia, jsonUtili, mappaWhere, mappaRisultati, grafoPriorita);
				//aggiornaWeights(grafoPrioritaCompatto, mappaWeights, mappaRisultati, foglia) devo avere coma parametro di esegui mappaWeights
//				grafoCopia.removeAllEdges(grafoCopia.incomingEdgesOf(foglia));
				grafoCopia.removeVertex(foglia);
			}
		}
		System.out.println("MAPPA RISULTATI = "+ mappaRisultati.toString());
//		mappaRisultati.toString().replaceAll(Pattern.quote("{\"store\":\"{"), "{");
//		System.out.println("MAPPA RISULTATI = "+ mappaRisultati.toString());
		esegui(grafoPrioritaCompatto, grafoCopia, grafoPriorita, jsonUtili, mappaWhere, mappaRisultati);
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

	//DA DECIDERE SE METTERLO QUA O IN CARICATORE JSON
	public List<List<String>> getFoglie(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> g){
		List<List<String>> foglie = new LinkedList<>();
		for(List<String> vertex: g.vertexSet())
			if(g.outgoingEdgesOf(vertex).size() == 0)
				foglie.add(vertex);
		return foglie;
	}

}
