package it.uniroma3.costruttoreQuery;

import java.util.List;
import java.util.Map;


import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import com.google.gson.JsonArray;

import it.uniroma3.JsonUtils.Convertitore;
import it.uniroma3.persistence.neo4j.GraphDao;
import org.neo4j.graphdb.Result;

public class CostruttoreQuerySQL implements CostruttoreQuery{

	@Override
	public void eseguiQuery(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto, List<String> nodo,
			Map<String, List<List<String>>> mappaWhere, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita) throws Exception {
		
		StringBuilder queryRiscritta = new StringBuilder();
		queryRiscritta.append("SELECT * FROM\n");
		int i = 0; //contatore di risultati con cui fare join
		for(DefaultWeightedEdge arco : grafoPrioritaCompatto.outgoingEdgesOf(nodo)){
			i++;
			JsonArray risFiglio = mappaRisultati.get(grafoPrioritaCompatto.getEdgeTarget(arco));
			queryRiscritta.append("json_array_elements('"+risFiglio+"') AS elem"+i+",\n");
			
//			OPZIONE 1::: (USATA)
//			
//			SELECT *
//			FROM   json_array_elements(
//			  '[ {     "rental_id" : 1,     "inventory_id" : 1,     "customer_id" : 1,     "staff_id" : 1   }, {     "rental_id" : 2,     "inventory_id" : 2,     "customer_id" : 1,     "staff_id" : 2   },{     "rental_id" : 3,     "inventory_id" : 7,     "customer_id" : 2,     "staff_id" : 2   },{     "rental_id" : 4,     "inventory_id" : 8,     "customer_id" : 2,     "staff_id" : 2   },{     "rental_id" : 5,     "inventory_id" : 3,     "customer_id" : 3,     "staff_id" : 3   }, {     "rental_id" : 6,     "inventory_id" : 4,     "customer_id" : 3,     "staff_id" : 3   },{     "rental_id" : 7,     "inventory_id" : 5,     "customer_id" : 4,     "staff_id" : 4   },{     "rental_id" : 8,     "inventory_id" : 6,     "customer_id" : 4,     "staff_id" : 4   }]
//			'
//			  ) AS elem, customer 
//			WHERE elem->>'customer_id' = customer.customer_id::text;

			
//			OPZIONE 2:::
//			
//			CREATE TEMP TABLE table_b(rental_id int, inventory_id int, customer_id int, staff_id int);
//			INSERT INTO table_b VALUES
//			  (1, 1, 1, 1)
//			, (2, 2, 1, 2)
//			, (3, 7, 2, 2)
//			, (4, 8, 2, 2)
//			, (5, 3, 3, 3)
//			, (6, 4, 3, 3)
//			, (7, 5, 4, 4)
//			, (8, 6, 4, 4);
//
//			SELECT * FROM table_b b, customer c WHERE b.customer_id = c.customer_id;
			
		}
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
				if(nodo.contains(condizione.get(1).split("\\.")[0])) //se la condizione è interna al nodo
					queryRiscritta.append("AND " + condizione.get(0) + " = " + condizione.get(1) +"\n");
				else{
					for(int k=0; k<i ;k++){ //se la condizione mi richiede di fare il join con uno dei risultati dei figli provo il join con tutti i figli e se non sono uguali i campi non mi aggiunge ennuple al risultato
						queryRiscritta.append("AND " + condizione.get(0) + "::text = elem"+k+"->>'" + condizione.get(0).split("\\.")[1] +"\n");
					}
				}
			}
		}
		System.out.println("QUERY FINALE SQL : \n"+queryRiscritta.toString());
		mappaRisultati.put(nodo, eseguiQueryDirettamente(queryRiscritta)); //da fare un flatten perchè il campo "value" contene il json risultato corrispondente
		System.out.println("RISULTATO INSERITO NELLA MAPPARISULTATI: "+eseguiQueryDirettamente(queryRiscritta));
		
	}
	
	private JsonArray eseguiQueryDirettamente(StringBuilder queryRiscritta) throws Exception{
		GraphDao dao = new GraphDao();
		Result risultatoResultSet = dao.interroga(queryRiscritta.toString());
		JsonArray risultati = Convertitore.convertCypherToJSON(risultatoResultSet);
		return risultati;
	}
}
