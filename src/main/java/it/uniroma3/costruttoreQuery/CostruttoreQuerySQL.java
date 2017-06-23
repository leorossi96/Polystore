package it.uniroma3.costruttoreQuery;

import java.io.StringReader;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import it.uniroma3.JsonUtils.Convertitore;
import it.uniroma3.persistence.postgres.RelationalDao;


public class CostruttoreQuerySQL implements CostruttoreQuery{

	/**
	 * Esempio di opzione utilizzata per iniettare JSON in query SQL

		SELECT *
		FROM   json_array_elements('[ {"rental_id" : 1,"inventory_id" : 1,"customer_id" : 1,"staff_id" : 1},
			{"rental_id" : 2,"inventory_id" : 2,"customer_id" : 1,"staff_id" : 2},
			{"rental_id" : 3,"inventory_id" : 7,"customer_id" : 2,"staff_id" : 2},
			{"rental_id" : 4,"inventory_id" : 8,"customer_id" : 2,"staff_id" : 2},
			{"rental_id" : 5,"inventory_id" : 3,"customer_id" : 3,"staff_id" : 3},
			{"rental_id" : 6,"inventory_id" : 4,"customer_id" : 3,"staff_id" : 3},
			{"rental_id" : 7,"inventory_id" : 5,"customer_id" : 4,"staff_id" : 4},
			{"rental_id" : 8,"inventory_id" : 6,"customer_id" : 4,"staff_id" : 4}]') 
			AS elem, customer 
		WHERE elem->>'customer_id' = customer.customer_id::text;
	 */
	@Override
	public void eseguiQuery(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto, List<String> nodo,
			Map<String, List<List<String>>> mappaWhere, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita) throws Exception {

		String campoSelect = this.getForeingKeyNodo(grafoPriorita,nodo.get(0),mappaWhere);
		if(campoSelect == null)
			campoSelect = "*";
		
		StringBuilder queryRiscritta = new StringBuilder();
		queryRiscritta.append("SELECT "+nodo.get(0)+"."+campoSelect+" FROM\n");
		int i = 0; //contatore di risultati con cui fare join
		for(DefaultWeightedEdge arco : grafoPrioritaCompatto.outgoingEdgesOf(nodo)){
			JsonArray risFiglio = mappaRisultati.get(grafoPrioritaCompatto.getEdgeTarget(arco));
			mappaRisultati.remove(grafoPrioritaCompatto.getEdgeTarget(arco));
			queryRiscritta.append("json_array_elements('"+risFiglio+"') AS elem"+i+",\n");
			i++;
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
						queryRiscritta.append("AND " + condizione.get(0) + "::text = elem"+k+"->>'" + condizione.get(0).split("\\.")[1] +"'\n");
					}
				}
			}
		}
		System.out.println("QUERY FINALE SQL : \n"+queryRiscritta.toString());
		JsonArray risultati = eseguiQueryDirettamente(queryRiscritta);
		JsonArray risutatiFormaCorretta = this.pulisciRisultati(risultati);
		mappaRisultati.put(nodo, risutatiFormaCorretta); //da fare un flatten perchè il campo "value" contene il json risultato corrispondente
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
		for(List<String> condizioni : mappaWhere.get(padre)){
			StringTokenizer st = new StringTokenizer(condizioni.get(1), ".");
			if(st.nextToken().equals(nodo))
				return st.nextToken();
		}
		return null;
	}

	private JsonArray pulisciRisultati(JsonArray ris) {
		StringBuilder sb = new StringBuilder();
		Iterator<JsonElement> iterator = ris.iterator();
		while (iterator.hasNext()) {
			sb.append(iterator.next().toString());
		}
		String risultati = sb.toString();
		System.out.println("RISULTATI ="+risultati+"\n");
		String r = risultati.replaceAll(Pattern.quote("\\\""), "\"")
				.replaceAll(Pattern.quote("{\"value\":\"{"), "{")
				.replaceAll(Pattern.quote("}\""), "")
				.replaceAll(":([^\"].*?),", ":\"$1\",")
				.replaceAll(Pattern.quote("}{"), "},{");
		String r2 = "["+r+"]";
		System.out.println("RISULTATI ="+r2); 
		StringReader s = new StringReader(r2);
		JsonReader j = new JsonReader(s);
		j.setLenient(true);
		return new JsonParser().parse(j).getAsJsonArray();
	}

	private JsonArray eseguiQueryDirettamente(StringBuilder queryRiscritta) throws Exception{
		RelationalDao dao = new RelationalDao();
		ResultSet risultatoResultSet = dao.interroga(queryRiscritta.toString());
		JsonArray risultati = Convertitore.convertSQLToJson(risultatoResultSet);
		return risultati;
	}
}
;