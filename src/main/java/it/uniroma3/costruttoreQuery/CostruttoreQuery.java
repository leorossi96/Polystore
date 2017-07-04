package it.uniroma3.costruttoreQuery;

import java.util.List;
import java.util.Map;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import com.google.gson.JsonArray;

public interface CostruttoreQuery {
	public void eseguiQuery(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto, List<String> nodo, Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita) throws Exception;

	public void eseguiQueryProiezione(List<String> fkUtili, List<String> nextNodoPath, List<String> nodoPath,
			List<String> nextNextNodoPath, Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect,
			Map<List<String>, JsonArray> mappaRisultati) throws Exception;

}
