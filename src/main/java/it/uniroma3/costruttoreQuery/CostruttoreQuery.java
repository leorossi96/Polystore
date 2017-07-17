package it.uniroma3.costruttoreQuery;

import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public abstract class CostruttoreQuery {
	public abstract void eseguiQuery(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto, List<String> nodo, Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita, Map<String, JsonObject> jsonUtili) throws Exception;

	public abstract void eseguiQueryProiezione(List<String> fkUtili, List<String> nextNodoPath, List<String> nextNextNodoPath,
			Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect,
			Map<List<String>, JsonArray> mappaRisultati, Map<String, JsonObject> jsonUtili) throws Exception;
	
	/**
	 * 
	 * @return La chiave esterna sulla quale il nodo padre effettua il join. Null altrimenti.
	 */
	protected String getForeignKeyNodo(SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita, String nodo, Map<String, List<List<String>>> mappaWhere) {
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

}
