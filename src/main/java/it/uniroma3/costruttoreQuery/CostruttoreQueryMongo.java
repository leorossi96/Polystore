package it.uniroma3.costruttoreQuery;

import java.sql.ResultSet;
import java.util.HashMap;
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
import it.uniroma3.json.Convertitore;
import it.uniroma3.json.ResultCleaner;
import it.uniroma3.persistence.mongo.MongoDao;

public class CostruttoreQueryMongo extends CostruttoreQuery {

	@Override
	public void eseguiQuery(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto,
			List<String> nodo, Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph <String, DefaultWeightedEdge> grafoPriorita, Map<String, JsonObject> jsonUtili)
					throws Exception {

		final long startTime = System.currentTimeMillis();

		Map<String , List<String>> mappaArrayFkFigli = this.getMappaArrayFkFigli(grafoPrioritaCompatto, mappaRisultati, nodo); 
		String campoSelect = this.getForeignKeyNodo(grafoPriorita,nodo.get(0),mappaWhere);
		//		System.out.println("CAMPO SELECT = "+campoSelect +"\n");
		List<String> listaProiezioniNodo = new LinkedList<>();
		StringBuilder queryRiscritta = new StringBuilder();
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
							if(!nodo.contains(tabellaFiglio)){
								String pk = jsonUtili.get(tabellaFiglio).get("primarykey").getAsString().split("\\.")[1];
								listaProiezioniNodo.add(tabella+"."+pk); //aggiungi alle proiezioni che già c'erano il fatto di ritornare l'id di ogni figlio per garantire il join
							}
						}
					}
				}
				queryRiscritta.append(listaProiezioniNodo.toString().replaceAll(Pattern.quote("["), "").replaceAll(Pattern.quote("]"), "")+"\nFROM\n");
			}
		}else
			queryRiscritta.append("SELECT DISTINCT "+campoSelect+"\nFROM\n");

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
					String valoriFkFiglio = mappaArrayFkFigli.get(condizione.get(1)).toString().replaceAll(Pattern.quote("["), "(").replaceAll(Pattern.quote("]"), ")");
					queryRiscritta.append("AND "+condizione.get(0)+" IN "+valoriFkFiglio+"\n");
				}
			}
		}

		String queryMongo = queryRiscritta.toString();
		System.out.println("QUERY MONGO ="+queryMongo+"\n");
		JsonArray risultati = eseguiQueryDirettamente(queryMongo);
		JsonArray risutatiFormaCorretta = ResultCleaner.fromMongo(risultati);
		mappaRisultati.put(nodo, risutatiFormaCorretta);
		final long elapsedTime = System.currentTimeMillis() - startTime;
		System.out.println("Tempo impiegato query Mongo " + elapsedTime/1000.0);
		//		System.out.println("RISULTATO INSERITO NELLA MAPPARISULTATI: "+ risutatiFormaCorretta.toString());

	}

	@Override
	public void eseguiQueryProiezione (List<String> fkUtili, List<String> nextNodoPath, List<String> nextNextNodoPath,
			Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect,
			Map<List<String>, JsonArray> mappaRisultati, Map<String, JsonObject> jsonUtili) throws Exception{

		final long startTime = System.currentTimeMillis();

		StringBuilder queryProiezione = new StringBuilder();
		queryProiezione.append("SELECT ");
		List<String> campiDaSelezionareDelNodo = new LinkedList<>();
		String pkNextNodoPath = jsonUtili.get(nextNodoPath.get(0)).get("primarykey").getAsString().split("\\.")[1];
		for(String tabella: nextNodoPath){
			if(mappaSelect.containsKey(tabella))	{
				//				System.out.println("TABELLA: "+tabella);
				//				System.out.println("GETTABELLA: "+mappaSelect.get(tabella));
				campiDaSelezionareDelNodo.addAll(mappaSelect.get(tabella));
			}
		}

		if(nextNextNodoPath != null){//vuol dire che è un nodo del path di passaggio
			String pkNextNextNodoPath = jsonUtili.get(nextNextNodoPath.get(0)).get("primarykey").getAsString().split("\\.")[1];
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
			queryProiezione.append(nextNodoPath.get(0)+"."+pkNextNodoPath+", "+tabellaDiJoin+"."+pkNextNextNodoPath+"\nFROM\n");
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
				System.out.println("QUERYPROIEZIONE");
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
		if(fkUtili!=null)	{
			List<String> fkUtili2 = new LinkedList<>();
			for(String s : fkUtili)
				fkUtili2.add("'"+s+"'");
			queryProiezione.append("AND "+nextNodoPath.get(0)+"."+pkNextNodoPath+" IN "+fkUtili2.toString().replaceAll(Pattern.quote("["), "(").replaceAll(Pattern.quote("]"), ")")+"\n");	
		}
		else{ //mi trovo nella radice nella seconda fase dell'esecuzione
			for(String tabella: nextNodoPath){
				String pkTabella = jsonUtili.get(tabella).get("primarykey").getAsString().split("\\.")[1];
				List<String> fkUtiliCampoRadice = new LinkedList<>();
				for(JsonElement je : mappaRisultati.get(nextNodoPath)){
					JsonObject jo = je.getAsJsonObject();
					fkUtiliCampoRadice.add(jo.get(pkTabella).getAsString());
				}
				queryProiezione.append("AND "+tabella+"."+pkTabella+"IN "+fkUtiliCampoRadice.toString().replaceAll(Pattern.quote("["), "(").replaceAll(Pattern.quote("]"), ")")+"\n");
			}
		}
		String query = queryProiezione.toString();
		System.out.println("\nQUERY PROIEZIONE MONGO ="+queryProiezione+"\n");
		JsonArray risultati = eseguiQueryDirettamente(query);
		JsonArray risutatiFormaCorretta = ResultCleaner.fromSQL(risultati);
		mappaRisultati.put(nextNodoPath, risutatiFormaCorretta);
		final long elapsedTime = System.currentTimeMillis() - startTime;
		System.out.println("Tempo impiegato query Mongo " + elapsedTime/1000.0);
		//		System.out.println("RISULTATO INSERITO NELLA MAPPARISULTATI: "+ risutatiFormaCorretta.toString());
	}

	/**
	 * creo una mappa che ha come chiave il nome della fk dei figli e come valore la lista delle fk da unsare nella funzione IN  
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

	private JsonArray eseguiQueryDirettamente(String query) throws Exception { 
		ResultSet risultatoResult = new MongoDao().interroga(query);
		JsonArray risultati = Convertitore.convertSQLMongoToJson(risultatoResult);
		return risultati;
	}
}
