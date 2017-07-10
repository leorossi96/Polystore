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
import it.uniroma3.persistence.neo4j.GraphDao;

public class CostruttoreQueryNeo4j extends CostruttoreQuery {

	@Override
	public void eseguiQuery(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto,
			List<String> nodo, Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita)
					throws Exception {
		Map<String , List<String>> mappaArrayFkFigli = this.getMappaArrayFkFigli(grafoPrioritaCompatto, mappaRisultati, nodo); //address.address_id ->["1","2"], customer.customer_id ->["4","9"]

		System.out.println("MAPPA ARRAY FK FIGLI = "+ mappaArrayFkFigli.toString());
		StringBuilder queryRiscritta = new StringBuilder();
		boolean isFiglio = true;
		boolean joinRisultati = false;
		List<String> listaProiezioniNodo = new LinkedList<>();

		String campoReturn = this.getForeignKeyNodo(grafoPriorita,nodo.get(0),mappaWhere);
		if(campoReturn == null){ // è la radice
			isFiglio = false;	
			for(String tabella : nodo){
				if(mappaSelect.get(tabella) != null && !mappaSelect.get(tabella).get(0).equals("*"))
					listaProiezioniNodo.addAll(mappaSelect.get(tabella));
			}
		}
		else
			listaProiezioniNodo.add(nodo.get(0)+"."+nodo.get(0)+"_id");

		String tabellaPartenza = nodo.get(0);
		queryRiscritta.append("MATCH ("+tabellaPartenza+" : "+tabellaPartenza+")\n");
		queryRiscritta.append("WHERE 1=1\n");
		for(int i=0; i<nodo.size(); i++){
			List<List<String>> condizioniTabella = mappaWhere.get(nodo.get(i));		
			for(int j=0; j<condizioniTabella.size(); j++){
				List<String> condizione = condizioniTabella.get(j);
				String primaParolaParametro = condizione.get(1).split("\\.")[0];
				if(nodo.contains(primaParolaParametro)) {//non è una condizione di join 
					joinRisultati = true;
					queryRiscritta.append("MATCH ("+nodo.get(i)+" : "+nodo.get(i)+")--("+primaParolaParametro+" : "+primaParolaParametro+")\n");
					queryRiscritta.append("WHERE 1=1\n");
				}
				else if(!mappaWhere.keySet().contains(primaParolaParametro))
					queryRiscritta.append("AND "+condizione.get(0)+" = "+condizione.get(1)+"\n");
				else {
					if(!this.condizioneStringente(mappaWhere, nodo.get(i), condizione.get(0)))
						queryRiscritta.append("AND "+condizione.get(0)+" IN "+mappaArrayFkFigli.get(condizione.get(1).toString())+"\n");
				}
			}
		}
		if(isFiglio)
			queryRiscritta.append("RETURN DISTINCT "+campoReturn);
		else {
			//			queryRiscritta.append("RETURN ");
			//			for(int i=0; i<nodo.size()-1; i++){
			//				queryRiscritta.append(nodo.get(i)+"."+nodo.get(i)+"_id"+", ");
			//			}
			//			queryRiscritta.append(nodo.get(nodo.size()-1)+"."+nodo.get(nodo.size()-1)+"_id");
			if(listaProiezioniNodo.isEmpty()){
				queryRiscritta.append("RETURN { ");
				for(int n=0; n<nodo.size()-1; n++){
					queryRiscritta.append(nodo.get(n)+" : "+nodo.get(n)+", ");
				}
				queryRiscritta.append(nodo.get(nodo.size()-1)+" : "+nodo.get(nodo.size()-1)+" }\n");
			}
			else{ // sono nella radice e ho delle condizioni da applicare
				if(!grafoPrioritaCompatto.outgoingEdgesOf(nodo).isEmpty()){ //ha dei figli
					for(String tabella: nodo){
						Iterator<DefaultWeightedEdge> i = grafoPriorita.outgoingEdgesOf(tabella).iterator();
						while(i.hasNext()){
							String tabellaFiglio = grafoPriorita.getEdgeTarget(i.next());
							if(!nodo.contains(tabellaFiglio))
								listaProiezioniNodo.add(tabella+"."+tabellaFiglio+"_id"); //aggiungi alle proiezioni che già c'erano il fatto di ritornare l'id di ogni figlio per garantire il join
						}
					}
					//				List<String> chiaviPerIFigli = new LinkedList<>();
					//				for(String tabella: nodo){
					//					chiaviPerIFigli.add(tabella+"."+tabella+"_id");
					//				}
					//				if(!listaProiezioniNodo.containsAll(chiaviPerIFigli))
					//					listaProiezioniNodo.addAll(chiaviPerIFigli);
					queryRiscritta.append("RETURN "+listaProiezioniNodo.toString().replaceAll(Pattern.quote("["), "").replaceAll(Pattern.quote("]"), ""));
				}
			}
		}
		String queryNeo4j = queryRiscritta.toString();
		System.out.println("QUERY NEO4J =\n"+ queryNeo4j);
		JsonArray risultati = eseguiQueryDirettamente(queryNeo4j, campoReturn, listaProiezioniNodo);
		JsonArray risutatiFormaCorretta = ResultCleaner.fromNeo4j(risultati, joinRisultati, isFiglio);
		mappaRisultati.put(nodo, risutatiFormaCorretta);		
	}

	@Override
	public void eseguiQueryProiezione(List<String> fkUtili, List<String> nextNodoPath, List<String> nextNextNodoPath,
			Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect,
			Map<List<String>, JsonArray> mappaRisultati) throws Exception {

		boolean isFiglio = false;
		boolean joinRisultati = false;
		StringBuilder queryProiezione = new StringBuilder();
		String tabellaPartenza = nextNodoPath.get(0);
		queryProiezione.append("MATCH ("+tabellaPartenza+" : "+tabellaPartenza+")\n");
		queryProiezione.append("WHERE 1=1\n");
		for(int i=0; i<nextNodoPath.size(); i++){
			List<List<String>> condizioniTabella = mappaWhere.get(nextNodoPath.get(i));		
			for(int j=0; j<condizioniTabella.size(); j++){
				List<String> condizione = condizioniTabella.get(j);
				String primaParolaParametro = condizione.get(1).split("\\.")[0];
				if(nextNodoPath.contains(primaParolaParametro)) {//non è una condizione di join 
					joinRisultati = true;
					queryProiezione.append("MATCH ("+nextNodoPath.get(i)+" : "+nextNodoPath.get(i)+")--("+primaParolaParametro+" : "+primaParolaParametro+")\n");
					queryProiezione.append("WHERE 1=1\n");
				}
				else if(!mappaWhere.keySet().contains(primaParolaParametro))
					queryProiezione.append("AND "+condizione.get(0)+" = "+condizione.get(1)+"\n");
			}			
		}
		if(fkUtili!=null)
			queryProiezione.append("AND "+nextNodoPath.get(0)+"."+nextNodoPath.get(0)+"_id"+" IN "+fkUtili.toString()+"\n");	
		else{//mi trovo nella radice nella seconda fase dell'esecuzione
			for(String tabella: nextNodoPath){
				List<String> fkUtiliCampoRadice = new LinkedList<>();
				for(JsonElement je : mappaRisultati.get(nextNodoPath)){
					JsonObject jo = je.getAsJsonObject();
					fkUtiliCampoRadice.add(jo.get(tabella+"_id").getAsString());
				}
				queryProiezione.append("AND "+tabella+"."+tabella+"_id IN "+fkUtiliCampoRadice.toString()+"\n");
			}
		}
		List<String> campiDaSelezionareDelNodo = new LinkedList<>();
		queryProiezione.append("RETURN \n");
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
			queryProiezione.append(nextNodoPath.get(0)+"."+nextNodoPath.get(0)+"_id, "+tabellaDiJoin+"."+nextNextNodoPath.get(0));
		}
		else{
			if(campiDaSelezionareDelNodo.get(0).equals("*")){
				queryProiezione.append("{");
				for(int n=0; n<nextNodoPath.size()-1; n++){
					queryProiezione.append(nextNodoPath.get(n)+" : "+nextNodoPath.get(n)+", ");
				}
				queryProiezione.append(nextNodoPath.get(nextNodoPath.size()-1)+" : "+nextNodoPath.get(nextNodoPath.size()-1)+"}\n");
				campiDaSelezionareDelNodo.clear();
			}else{
				if(!campiDaSelezionareDelNodo.contains(nextNodoPath.get(0)+"."+nextNodoPath.get(0)+"_id")) 
					campiDaSelezionareDelNodo.add(nextNodoPath.get(0)+"."+nextNodoPath.get(0)+"_id"); // per permettermi di comporre il risultato finale in seguito
				for(int i=0; i<campiDaSelezionareDelNodo.size()-1; i++){
					queryProiezione.append(campiDaSelezionareDelNodo.get(i)+", ");
				}
				queryProiezione.append(campiDaSelezionareDelNodo.get(campiDaSelezionareDelNodo.size()-1));
			}
		}

		String queryNeo4j = queryProiezione.toString();
		System.out.println("QUERY NEO4J PROIEZIONE=\n"+ queryNeo4j);
		JsonArray risultati = eseguiQueryDirettamente(queryNeo4j, null, campiDaSelezionareDelNodo);
		JsonArray risutatiFormaCorretta = ResultCleaner.fromNeo4j(risultati, joinRisultati, isFiglio);
		mappaRisultati.put(nextNodoPath, risutatiFormaCorretta);

	}

	private boolean condizioneStringente(Map<String, List<List<String>>> mappaWhere, String tabellaCorrente, String valoreCondizione) {
		boolean piùStringente = false;
		List<List<String>> condizioniTabella = mappaWhere.get(tabellaCorrente);
		for(int i=0; i<condizioniTabella.size(); i++){
			List<String> condizione = condizioniTabella.get(i);
			if(condizione.get(0).equals(valoreCondizione)){
				String primaParolaParametro = condizione.get(1).split("\\.")[0];
				if(!mappaWhere.keySet().contains(primaParolaParametro)){
					piùStringente = true;
					return piùStringente;
				}
			}
		}
		return piùStringente;
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

	private JsonArray eseguiQueryDirettamente(String query, String campoReturn, List<String> listaProiezioniNodo) throws Exception{
		ResultSet risultatoResult = new GraphDao().interroga(query);
		JsonArray risultati = Convertitore.convertCypherToJson(risultatoResult, campoReturn, listaProiezioniNodo);
		return risultati;
	}
}
