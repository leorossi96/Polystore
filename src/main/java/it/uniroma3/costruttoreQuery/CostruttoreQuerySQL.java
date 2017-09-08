package it.uniroma3.costruttoreQuery;

import java.io.StringReader;
import java.sql.ResultSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;
import com.google.gson.stream.JsonReader;

import it.uniroma3.json.Convertitore;
import it.uniroma3.json.ResultCleaner;
import it.uniroma3.persistence.postgres.RelationalDao;

public class CostruttoreQuerySQL extends CostruttoreQuery{

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
			Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita, Map<String, JsonObject> jsonUtili) throws Exception {

		final long startTime = System.currentTimeMillis();

		String campoSelect = this.getForeignKeyNodo(grafoPriorita,nodo.get(0),mappaWhere);
		//System.out.println("CAMPO SELECT = "+campoSelect +"\n");
		StringBuilder queryRiscritta = new StringBuilder();
		List<String> listaProiezioniNodo = new LinkedList<>();
		if(campoSelect == null){ //è la radice
			queryRiscritta.append("SELECT\n");
			for(String tabella : nodo){
				if(mappaSelect.get(tabella) != null && !mappaSelect.get(tabella).get(0).equals("*"))
					listaProiezioniNodo.addAll(mappaSelect.get(tabella));
			}
			if(listaProiezioniNodo.isEmpty()){
				queryRiscritta.append("*\nFROM\n");
			}
			else{ // sono nella radice e ho delle proiezioni da applicare
				if(!grafoPrioritaCompatto.outgoingEdgesOf(nodo).isEmpty()){ //ha dei figli
					for(String tabella: nodo){
						Iterator<DefaultWeightedEdge> i = grafoPriorita.outgoingEdgesOf(tabella).iterator();
						while(i.hasNext()){
							String tabellaFiglio = grafoPriorita.getEdgeTarget(i.next());
							String pkTabellaFiglio = jsonUtili.get(tabellaFiglio).get("primarykey").getAsString().split("\\.")[1]; 
							if(!nodo.contains(tabellaFiglio))
								listaProiezioniNodo.add(tabella+"."+pkTabellaFiglio); //aggiungi alle proiezioni che già c'erano il fatto di ritornare l'id di ogni figlio per garantire il join
						}
					}
				}
				queryRiscritta.append(listaProiezioniNodo.toString().replaceAll(Pattern.quote("["), "").replaceAll(Pattern.quote("]"), "")+"\nFROM\n");
			}
		}
		else
			queryRiscritta.append("SELECT DISTINCT "+campoSelect+"\nFROM\n");

		int i = 0; //contatore di risultati con cui fare join
		for(DefaultWeightedEdge arco : grafoPrioritaCompatto.outgoingEdgesOf(nodo)){
			JsonArray risFiglio1 = mappaRisultati.get(grafoPrioritaCompatto.getEdgeTarget(arco));
			JsonElement temp;
			JsonArray risFiglio = new JsonArray();
			for(int j = 0; j<risFiglio1.size();j++) {

				temp = risFiglio1.get(j);
				String s = temp.toString().split(":")[1].replaceAll("\"", "").replaceAll("}", "");
				float a = Float.parseFloat(s);
				String s1 = temp.toString().split(":")[0] + ":\"" + (int)a +"\"}";
				JsonReader jreader = new JsonReader(new StringReader(s1));
				jreader.setLenient(true);
				JsonElement el = new JsonParser().parse(jreader);
				risFiglio.add(el);
			}
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
				String primaParolaParametro = condizione.get(1).split("\\.")[0];
				if(nodo.contains(primaParolaParametro) || !mappaWhere.keySet().contains(primaParolaParametro)) //se la condizione è interna al nodo
					queryRiscritta.append("AND " + condizione.get(0) + " = " + condizione.get(1) +"\n");
				else{
					for(int k=0; k<i ;k++){ 
						queryRiscritta.append("AND " + condizione.get(0) + "::text = elem"+k+"->>'" + condizione.get(1).split("\\.")[1] +"'\n");
					}
				}
			}
		}
		String querySQL = queryRiscritta.toString();
		System.out.println("QUERY SQL");
		JsonArray risultati = eseguiQueryDirettamente(querySQL);
		JsonArray risutatiFormaCorretta = ResultCleaner.fromSQL(risultati);
		mappaRisultati.put(nodo, risutatiFormaCorretta);
		final long elapsedTime = System.currentTimeMillis() - startTime;
		System.out.println("Tempo impiegato query SQL " + elapsedTime/1000.0);
		//		System.out.println("RISULTATO INSERITO NELLA MAPPARISULTATI: "+ risutatiFormaCorretta.toString());
	}

	@Override
	public void eseguiQueryProiezione (List<String> fkUtili, List<String> nextNodoPath, List<String> nextNextNodoPath,
			Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect,
			Map<List<String>, JsonArray> mappaRisultati, Map<String, JsonObject> jsonUtili) throws Exception{

		final long startTime = System.currentTimeMillis();

		String pkNextNodoPath = jsonUtili.get(nextNodoPath).get("primarykey").getAsString().split("\\.")[1];

		StringBuilder queryProiezione = new StringBuilder();
		queryProiezione.append("SELECT ");
		List<String> campiDaSelezionareDelNodo = new LinkedList<>();
		for(String tabella: nextNodoPath){
			if(mappaSelect.get(tabella) != null)
				campiDaSelezionareDelNodo.addAll(mappaSelect.get(tabella));
		}

		if(nextNextNodoPath != null){//vuol dire che è un nodo del path di passaggio
			String tabellaDiJoin = null;
			for(String tabella : nextNodoPath){
				for(List<String> condizioneTabella: mappaWhere.get(tabella)){
					if(condizioneTabella.get(1).split("\\.")[1].contains(nextNextNodoPath.get(0)))
						tabellaDiJoin = tabella;
				}
			}
			for(int i=0; i<campiDaSelezionareDelNodo.size()-1; i++){
				queryProiezione.append(campiDaSelezionareDelNodo.get(i)+", ");
			}
			queryProiezione.append(campiDaSelezionareDelNodo.get(campiDaSelezionareDelNodo.size()-1)+", ");
			queryProiezione.append(nextNodoPath.get(0)+"."+pkNextNodoPath+", "+tabellaDiJoin+"."+pkNextNodoPath+"\nFROM\n");
		}
		else{
			if(campiDaSelezionareDelNodo.get(0).equals("*")){
				queryProiezione.append("*\nFROM\n");
			}
			else{
				if(!campiDaSelezionareDelNodo.contains(nextNodoPath.get(0)+"."+pkNextNodoPath)) 
					campiDaSelezionareDelNodo.add(nextNodoPath.get(0)+"."+pkNextNodoPath); // per permettermi di comporre il risultato finale in seguito
				for(int i=0; i<campiDaSelezionareDelNodo.size()-1; i++){
					queryProiezione.append(campiDaSelezionareDelNodo.get(i)+", ");
				}
				queryProiezione.append(campiDaSelezionareDelNodo.get(campiDaSelezionareDelNodo.size()-1)+"\nFROM\n");
			}
		}

		for(int z=0; z<nextNodoPath.size()-1; z++)
			queryProiezione.append(nextNodoPath.get(z)+",\n");
		queryProiezione.append(nextNodoPath.get(nextNodoPath.size()-1)+"\nWHERE\n1=1\n");


		for (String tabella: nextNodoPath){
			List<List<String>> condizioniTabella = mappaWhere.get(tabella);
			for(int j=0; j<condizioniTabella.size(); j++){
				List<String> condizione = condizioniTabella.get(j);
				String primaParolaParametro = condizione.get(1).split("\\.")[0];
				if(nextNodoPath.contains(primaParolaParametro) || !mappaWhere.keySet().contains(primaParolaParametro)) //se la condizione è interna al nodo
					queryProiezione.append("AND " + condizione.get(0) + " = " + condizione.get(1) +"\n");
			}
		}
		if(fkUtili!=null)
			queryProiezione.append("AND "+nextNodoPath.get(0)+"."+pkNextNodoPath+" IN "+fkUtili.toString().replaceAll(Pattern.quote("["), "(").replaceAll(Pattern.quote("]"), ")")+"\n");	

		else{ //mi trovo nella radice nella seconda fase dell'esecuzione
			for(String tabella: nextNodoPath){
				String fkTabella = jsonUtili.get(tabella).get("primarykey").getAsString().split("\\.")[1];
				List<String> fkUtiliCampoRadice = new LinkedList<>();
				for(JsonElement je : mappaRisultati.get(nextNodoPath)){
					JsonObject jo = je.getAsJsonObject();
					fkUtiliCampoRadice.add(jo.get(tabella+"_id").getAsString());
				}
				queryProiezione.append("AND "+tabella+"."+fkTabella+" IN "+fkUtiliCampoRadice.toString().replaceAll(Pattern.quote("["), "(").replaceAll(Pattern.quote("]"), ")")+"\n");
			}
		}
		String query = queryProiezione.toString();
		System.out.println("QUERY PROIEZIONE SQL");
		JsonArray risultati = eseguiQueryDirettamente(query);
		JsonArray risutatiFormaCorretta = ResultCleaner.fromSQL(risultati);
		mappaRisultati.put(nextNodoPath, risutatiFormaCorretta);
		final long elapsedTime = System.currentTimeMillis() - startTime;
		System.out.println("Tempo impiegato query SQL " + elapsedTime/1000.0);
		//		System.out.println("RISULTATO INSERITO NELLA MAPPARISULTATI: "+ risutatiFormaCorretta.toString());
	}

	private JsonArray eseguiQueryDirettamente(String query) throws Exception{
		ResultSet risultatoResultSet = new RelationalDao().interroga(query);
		JsonArray risultati = Convertitore.convertSQLMongoToJson(risultatoResultSet);
		return risultati;
	}
}
