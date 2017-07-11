package it.uniroma3.polystoreMain;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.jgrapht.alg.DijkstraShortestPath;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import it.uniroma3.costruttoreQuery.CostruttoreQuery;
import it.uniroma3.costruttoreQuery.CostruttoreQueryMongo;
import it.uniroma3.costruttoreQuery.CostruttoreQueryNeo4j;
import it.uniroma3.costruttoreQuery.CostruttoreQuerySQL;
import it.uniroma3.grafiPriotita.FabbricatoreAlberoEsecuzione;

@SuppressWarnings("deprecation")
public class WorkflowManager {

	public void esegui(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto, SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoCopia, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita, Map<String, JsonObject> jsonUtili, Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect, Map<List<String>, JsonArray> mappaRisultati) throws Exception{
		List<List<String>> foglie = this.getFoglie(grafoCopia);
		if(foglie.size() == 0) //radice
			return;
		else{
			Iterator<List<String>> i = foglie.iterator();
			while(i.hasNext()){
				List<String> foglia = i.next();
				eseguiQuery(grafoPrioritaCompatto, foglia, jsonUtili, mappaWhere, mappaSelect, mappaRisultati, grafoPriorita);
				grafoCopia.removeVertex(foglia);
			}
		}
		esegui(grafoPrioritaCompatto, grafoCopia, grafoPriorita, jsonUtili, mappaWhere, mappaSelect, mappaRisultati);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void eseguiProiezioni(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto, Map<String, List<String>> mappaSelect, Map<List<String>, JsonArray> mappaRisultati, Map<String, List<String>> mappaDB, Map<String, List<List<String>>> mappaWhere) throws Exception{
		FabbricatoreAlberoEsecuzione fae = new FabbricatoreAlberoEsecuzione();
		List<String> radice = fae.getRadice(grafoPrioritaCompatto);
		Iterator<List<String>> i = grafoPrioritaCompatto.vertexSet().iterator();
		List<String> nextNextNodoPath;
		while(i.hasNext()){
			boolean nodoSuCuiFareProiezioni = false;
			List<String> nodo = i.next();
			for(String tabellaNodo : nodo){
				if(!radice.get(0).equals(nodo.get(0)) && mappaSelect.keySet().contains(tabellaNodo)){
					nodoSuCuiFareProiezioni = true;
				}
			}
			if(nodoSuCuiFareProiezioni){
				List<List<String>> path = new DijkstraShortestPath(grafoPrioritaCompatto, radice, nodo).getPath().getVertexList();
				List<String> fkUtili = new LinkedList<>();
				if(path.size() == 2){
					List<String> nodoPath = path.get(0);
					List<String> nextNodoPath = path.get(1);
					JsonArray risultati = mappaRisultati.get(nodoPath);
					for(JsonElement je : risultati){
						JsonObject jo = je.getAsJsonObject();
						fkUtili.add(jo.get(nextNodoPath.get(0)+"_id").getAsString());
					}
					eseguiQueryProiezione(fkUtili, nextNodoPath, null, mappaRisultati, mappaDB, mappaWhere, mappaSelect);
				}
				else{
					for(int j=0; j<path.size()-2; j++){
						List<String> nodoPath = path.get(j);
						List<String> nextNodoPath = path.get(j+1);
						nextNextNodoPath = path.get(j+2);
						JsonArray risultati = mappaRisultati.get(nodoPath);
						for(JsonElement je : risultati){
							JsonObject jo = je.getAsJsonObject();
							fkUtili.add(jo.get(nextNodoPath.get(0)+"_id").getAsString());
						}
						eseguiQueryProiezione(fkUtili, nextNodoPath, nextNextNodoPath,mappaRisultati, mappaDB, mappaWhere, mappaSelect);
						fkUtili.clear();
					}
					nextNextNodoPath = null;
					List<String> nodoPath = path.get(path.size()-2);
					List<String> nextNodoPath = path.get(path.size()-1);
					JsonArray risultati = mappaRisultati.get(nodoPath);
					for(JsonElement je : risultati){
						JsonObject jo = je.getAsJsonObject();
						fkUtili.add(jo.get(nextNodoPath.get(0)+"_id").getAsString());
					}
					eseguiQueryProiezione(fkUtili, nextNodoPath, nextNextNodoPath,mappaRisultati, mappaDB, mappaWhere, mappaSelect);
				}
			}
		}
	}


	private void eseguiQueryProiezione(List<String> fkUtili, List<String> nextNodoPath, List<String> nextNextNodoPath,
			Map<List<String>, JsonArray> mappaRisultati, Map<String, List<String>> mappaDB, Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect) throws Exception {
		CostruttoreQuery costruttoreQuery = null;
		String dbNodo = null;
		for(String db : mappaDB.keySet()){
			if(mappaDB.get(db).contains(nextNodoPath.get(0)))
				dbNodo = db;
		}
		if(dbNodo.equalsIgnoreCase("postgreSQL"))
			costruttoreQuery = new CostruttoreQuerySQL();
		if(dbNodo.equalsIgnoreCase("mongoDB"))
			costruttoreQuery = new CostruttoreQueryMongo();
		if(dbNodo.equalsIgnoreCase("neo4j"))
			costruttoreQuery = new CostruttoreQueryNeo4j();
		costruttoreQuery.eseguiQueryProiezione(fkUtili, nextNodoPath, nextNextNodoPath, mappaWhere, mappaSelect, mappaRisultati);
	}

	private void eseguiQuery(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto, List<String> nodo, Map<String, JsonObject> jsonUtili,
			Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita) throws Exception {
		CostruttoreQuery costruttoreQuery = null;
		JsonObject myJson = jsonUtili.get(nodo.get(0)).getAsJsonObject();
		if(myJson.get("database").getAsString().equalsIgnoreCase("postgreSQL"))
			costruttoreQuery = new CostruttoreQuerySQL();
		if(myJson.get("database").getAsString().equalsIgnoreCase("mongoDB"))
			costruttoreQuery = new CostruttoreQueryMongo();
		if(myJson.get("database").getAsString().equalsIgnoreCase("neo4j"))
			costruttoreQuery = new CostruttoreQueryNeo4j();
		costruttoreQuery.eseguiQuery(grafoPrioritaCompatto, nodo, mappaWhere, mappaSelect, mappaRisultati, grafoPriorita);
	}

	private List<List<String>> getFoglie(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> g){
		List<List<String>> foglie = new LinkedList<>();
		for(List<String> vertex: g.vertexSet())
			if(g.outgoingEdgesOf(vertex).size() == 0)
				foglie.add(vertex);
		return foglie;
	}
}
