package it.uniroma3.costruttoreQuery;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import com.google.gson.JsonArray;
import it.uniroma3.JsonUtils.Convertitore;
import it.uniroma3.persistence.mongo.MongoDao;

public class CostruttoreQueryMongo implements CostruttoreQuery {

	
	@Override
	public void eseguiQuery(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto,
			List<String> nodo, Map<String, List<List<String>>> mappaWhere, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph <String, DefaultWeightedEdge> grafoPriorita)
					throws Exception {
	//TODO fare metodo creando una query SQL
	}

	private JsonArray eseguiQueryDirettamente(String queryRiscritta) throws Exception { 
		ResultSet risultatoResult = new MongoDao().interroga(queryRiscritta.toString());
		//TODO inserire convertitore adatto
		JsonArray risultati = Convertitore.convertCypherToJson(risultatoResult);
		return risultati;

	}
}
