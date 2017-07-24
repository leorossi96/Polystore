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
			List<String> nodo, Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita, Map<String, JsonObject> jsonUtili)
					throws Exception {

		final long startTime = System.currentTimeMillis();

		Map<String , List<String>> mappaArrayFkFigli = this.getMappaArrayFkFigli(grafoPrioritaCompatto, mappaRisultati, nodo); 
		//		System.out.println("MAPPA ARRAY FK FIGLI = "+ mappaArrayFkFigli.toString());
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
		else{
			String pk = jsonUtili.get(nodo.get(0)).get("primarykey").getAsString().split("\\.")[1];
			listaProiezioniNodo.add(nodo.get(0)+"."+pk);
		}

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
					queryRiscritta.append("AND "+condizione.get(0)+" = "+"'"+condizione.get(1)+"'"+"\n");
				else {
					if(!this.condizioneStringente(mappaWhere, nodo.get(i), condizione.get(0))){
						List<String> fkUtili = new LinkedList<>();
						if(mappaArrayFkFigli.get(condizione.get(1)) != null){
							for(String s: mappaArrayFkFigli.get(condizione.get(1).toString())){
								fkUtili.add("'"+s+"'");
							}
							queryRiscritta.append("AND "+condizione.get(0)+" IN "+fkUtili.toString()+"\n");
						}
						else
							queryRiscritta.append("AND "+condizione.get(0)+" IN [ ]\n");
					}
				}
			}
		}
		if(isFiglio)
			queryRiscritta.append("RETURN DISTINCT "+campoReturn);
		else {
			if(listaProiezioniNodo.isEmpty()){
				//				queryRiscritta.append("RETURN {");
				//				for(int n=0; n<nodo.size()-1; n++){
				//					queryRiscritta.append(nodo.get(n)+" : "+nodo.get(n)+", ");
				//				}
				//				queryRiscritta.append(nodo.get(nodo.size()-1)+" : "+nodo.get(nodo.size()-1)+" }\n");
				queryRiscritta.append("RETURN ");
				for(int n=0; n<nodo.size()-1; n++){
					JsonArray membri = jsonUtili.get(nodo.get(n)).getAsJsonArray("members");
					for(JsonElement membro : membri){
						queryRiscritta.append(membro.getAsString().replaceAll("\"", "")+",");
					}
				}
				JsonArray membri = jsonUtili.get(nodo.get(nodo.size()-1)).getAsJsonArray("members");
				for(int i=0; i<membri.size()-1; i++){
					String membro = membri.get(i).getAsString().replaceAll("\"", "");
					queryRiscritta.append(membro+",");
				}
				queryRiscritta.append(membri.get(membri.size()-1).getAsString().replaceAll("\"", ""));
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
				queryRiscritta.append("RETURN "+listaProiezioniNodo.toString().replaceAll(Pattern.quote("["), "").replaceAll(Pattern.quote("]"), ""));
			}
		}
		String queryNeo4j = queryRiscritta.toString();
		System.out.println("QUERY NEO4J =\n"+queryNeo4j+"\n");
		final long startTime2 = System.currentTimeMillis();
		JsonArray risultati = eseguiQueryDirettamente(queryNeo4j, campoReturn, listaProiezioniNodo);
		final long elapsedTime2 = System.currentTimeMillis() - startTime2;
		System.out.println("ESEGUITA "+ elapsedTime2/1000.0);
		JsonArray risutatiFormaCorretta = ResultCleaner.fromNeo4j(risultati, joinRisultati, isFiglio);
		System.out.println("PULITI");
		mappaRisultati.put(nodo, risutatiFormaCorretta);
		final long elapsedTime = System.currentTimeMillis() - startTime;
		System.out.println("Tempo impiegato query Neo4j " + elapsedTime/1000.0);
		//		System.out.println("RISULTATO INSERITO NELLA MAPPARISULTATI: "+ risutatiFormaCorretta.toString());

	}

	@Override
	public void eseguiQueryProiezione(List<String> fkUtili, List<String> nextNodoPath, List<String> nextNextNodoPath,
			Map<String, List<List<String>>> mappaWhere, Map<String, List<String>> mappaSelect,
			Map<List<String>, JsonArray> mappaRisultati, Map<String, JsonObject> jsonUtili) throws Exception {

		final long startTime = System.currentTimeMillis();
		String pkNextNodoPath = jsonUtili.get(nextNodoPath.get(0)).get("primarykey").getAsString().split("\\.")[1];
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
		if(fkUtili!=null){
			List<String> fkUtiliCorrette = new LinkedList<>();
			for (String s : fkUtili){
				fkUtiliCorrette.add("'"+s+"'");
			}
			queryProiezione.append("AND "+nextNodoPath.get(0)+"."+pkNextNodoPath+" IN "+fkUtiliCorrette.toString()+"\n");	
		}
		else{//mi trovo nella radice nella seconda fase dell'esecuzione
			for(String tabella: nextNodoPath){
				String pkTabella = jsonUtili.get(tabella).get("primarykey").getAsString().split("\\.")[1];
				List<String> fkUtiliCampoRadice = new LinkedList<>();
				for(JsonElement je : mappaRisultati.get(nextNodoPath)){
					JsonObject jo = je.getAsJsonObject();
					String elemento = jo.get(pkTabella).getAsString();
					fkUtiliCampoRadice.add(elemento); 
				}
				queryProiezione.append("AND "+tabella+"."+pkTabella+" IN "+fkUtiliCampoRadice.toString()+"\n");
			}
		}
		List<String> campiDaSelezionareDelNodo = new LinkedList<>();
		queryProiezione.append("RETURN \n");
		for(String tabella: nextNodoPath){
			if(mappaSelect.get(tabella) != null)
				campiDaSelezionareDelNodo.addAll(mappaSelect.get(tabella));
		}
		String campoJoinPadre = nextNodoPath.get(0)+"."+pkNextNodoPath;
		if (!campiDaSelezionareDelNodo.contains(campoJoinPadre))
			campiDaSelezionareDelNodo.add(campoJoinPadre);

		if(nextNextNodoPath != null){//vuol dire che è un nodo del path di passaggio
			String tabellaDiJoin = null;
			for(String tabella : nextNodoPath){
				for(List<String> condizioneTabella: mappaWhere.get(tabella)){
					if(condizioneTabella.get(1).split("\\.")[0].contains(nextNextNodoPath.get(0)))
						tabellaDiJoin = tabella;
				}
			}
			String campoJoinFiglio =tabellaDiJoin+"."+pkNextNodoPath;
			if (!campiDaSelezionareDelNodo.contains(campoJoinFiglio))
				campiDaSelezionareDelNodo.add(campoJoinFiglio);
			if(campiDaSelezionareDelNodo.get(0).equals("*")){
				//				queryProiezione.append("{");
				//				for(int n=0; n<nextNodoPath.size()-1; n++){
				//					queryProiezione.append(nextNodoPath.get(n)+" : "+nextNodoPath.get(n)+", ");
				//				}
				//				queryProiezione.append(nextNodoPath.get(nextNodoPath.size()-1)+" : "+nextNodoPath.get(nextNodoPath.size()-1)+"}\n");
				for(int n=0; n<nextNodoPath.size()-1; n++){
					JsonArray membri = jsonUtili.get(nextNodoPath.get(n)).getAsJsonArray();
					for(JsonElement membro : membri){
						queryProiezione.append(membro.getAsString()+",");
					}
				}
				JsonArray membri = jsonUtili.get(nextNodoPath.get(nextNodoPath.size()-1)).getAsJsonArray("members");
				for(int i=0; i<membri.size()-1; i++){
					String membro = membri.get(i).getAsString().replaceAll("\"", "");
					queryProiezione.append(membro+",");
				}
				queryProiezione.append(membri.get(membri.size()-1).getAsString().replaceAll("\"", ""));
				campiDaSelezionareDelNodo.clear();
			}else{
				for(int i=0; i<campiDaSelezionareDelNodo.size()-1; i++)
					queryProiezione.append(campiDaSelezionareDelNodo.get(i)+", ");
				queryProiezione.append(campiDaSelezionareDelNodo.get(campiDaSelezionareDelNodo.size()-1));
			}
		}
		else{
			if(campiDaSelezionareDelNodo.get(0).equals("*")){
				//				queryProiezione.append("{");
				//				for(int n=0; n<nextNodoPath.size()-1; n++){
				//					queryProiezione.append(nextNodoPath.get(n)+" : "+nextNodoPath.get(n)+", ");
				//				}
				//				queryProiezione.append(nextNodoPath.get(nextNodoPath.size()-1)+" : "+nextNodoPath.get(nextNodoPath.size()-1)+"}\n");
				for(int n=0; n<nextNodoPath.size()-1; n++){
					JsonArray membri = jsonUtili.get(nextNodoPath.get(n)).getAsJsonArray();
					for(JsonElement membro : membri){
						queryProiezione.append(membro.getAsString()+",");
					}
				}
				JsonArray membri = jsonUtili.get(nextNodoPath.get(nextNodoPath.size()-1)).getAsJsonArray("members");
				for(int i=0; i<membri.size()-1; i++){
					String membro = membri.get(i).getAsString().replaceAll("\"", "");
					queryProiezione.append(membro+",");
				}
				queryProiezione.append(membri.get(membri.size()-1).getAsString().replaceAll("\"", ""));
				campiDaSelezionareDelNodo.clear();
			}else{
				if(!campiDaSelezionareDelNodo.contains(nextNodoPath.get(0)+"."+pkNextNodoPath)) 
					campiDaSelezionareDelNodo.add(nextNodoPath.get(0)+"."+pkNextNodoPath); // per permettermi di comporre il risultato finale in seguito
				for(int i=0; i<campiDaSelezionareDelNodo.size()-1; i++){
					queryProiezione.append(campiDaSelezionareDelNodo.get(i)+", ");
				}
				queryProiezione.append(campiDaSelezionareDelNodo.get(campiDaSelezionareDelNodo.size()-1));
			}
		}

		String queryNeo4j = queryProiezione.toString();
		System.out.println("QUERY NEO4J PROIEZIONE = \n"+ queryProiezione);
		JsonArray risultati = eseguiQueryDirettamente(queryNeo4j, null, campiDaSelezionareDelNodo);
		JsonArray risutatiFormaCorretta = ResultCleaner.fromNeo4j(risultati, joinRisultati, isFiglio);
		mappaRisultati.put(nextNodoPath, risutatiFormaCorretta);
		final long elapsedTime = System.currentTimeMillis() - startTime;
		System.out.println("Tempo impiegato query Neo4j " + elapsedTime/1000.0);
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
