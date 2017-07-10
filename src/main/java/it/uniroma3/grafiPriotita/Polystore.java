package it.uniroma3.grafiPriotita;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;
import java.util.regex.Pattern;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import it.uniroma3.exeptions.MalformedQueryException;
import it.uniroma3.queryParser.ParserMongo;
import it.uniroma3.queryParser.ParserNeo4j;
import it.uniroma3.queryParser.ParserSql;
import it.uniroma3.queryParser.QueryParser;

public class Polystore {
	



	/*      ************************************* NON USATO *************************************
	public static List<String> getTabellaPrioritaAlta(List<String> tabelle, Map<String, JsonObject> jsonUtili) {

		if (tabelle.size()==1)
			return tabelle;
		List<String> tabellePreferite = new LinkedList<>();
		if (allEquals(tabelle) == true){
			tabellePreferite.add(tabelle.get(0));		
		}
		for(int i=0; i<tabelle.size();i++){
			JsonObject oggetto = jsonUtili.get(tabelle.get(i)); //es customer
			JsonArray knows = oggetto.get("knows").getAsJsonArray(); //es store e address
			for(int j=0; j<tabelle.size();j++){
				String tabella = tabelle.get(j);//customer
				for (int y=0; y<knows.size();y++){
					JsonObject tableKnows = knows.get(y).getAsJsonObject();//es store
					if (tableKnows.get("table").getAsString().equals(tabella)){//vuol dire che la conosce
						String tabellaPreferita = oggetto.get("table").getAsString();
						tabellePreferite.add(tabellaPreferita);		
					}
				}
			}
		}	
		while(tabellePreferite.size()>1){
			tabellePreferite = getTabellaPrioritaAlta(tabellePreferite, jsonUtili);
		}
		return tabellePreferite;
	}

	private static boolean allEquals(List<String> tabelle) {
		boolean allEquals = true;
		for (String s : tabelle) {
			if(!s.equals(tabelle.get(0)))
				allEquals = false;
		}
		return allEquals;
	}
	*/	

	/**
	 * 
	 * @param query La query da parsare
	 * @return Il giusto parser per la query data
	 * @throws MalformedQueryException
	 */
	private QueryParser identificaQuery(String query) throws MalformedQueryException	{
		if (query.toLowerCase().startsWith("select"))
			return new ParserSql();
		else if (query.toLowerCase().startsWith("match"))
			return new ParserNeo4j();
		else if (query.toLowerCase().startsWith("db."))
			return new ParserMongo();
		throw new MalformedQueryException("La query in input non è SQL, Cypher o Mongo");

	}

	private QueryParser getParser(String query) throws Exception {
		QueryParser parser = this.identificaQuery(query);
		return parser;
	}

	private JsonArray effettuaJoinRisultatoFinale(Map<List<String>, JsonArray> mappaRisultati,
			Map<String, List<String>> mappaSelect, List<String> radice, List<String> listaProiezioni) {
		int size = mappaRisultati.get(radice).size();
		//poi inserisco gli altri risultati (solo i campi utili)
		for(String tabellaRisultati : mappaSelect.keySet()){
			for(List<String> nodo : mappaRisultati.keySet()){
				if(!nodo.get(0).equals(radice.get(0)) && nodo.contains(tabellaRisultati) && mappaRisultati.get(nodo).size()==size){ //aggiungo uno ad uno
					for(int i=0; i<size; i++){
						JsonObject ennuplaRisultatoRadice = mappaRisultati.get(radice).get(i).getAsJsonObject();
						JsonObject ennuplaRisultatoNodo = mappaRisultati.get(nodo).get(i).getAsJsonObject();
						for(Entry<String, JsonElement> entry :ennuplaRisultatoNodo.entrySet()){
							if(mappaSelect.get(tabellaRisultati).get(0).equals("*") || mappaSelect.get(tabellaRisultati).contains(tabellaRisultati+"."+entry.getKey())) //aggiunge solo i campi richiesti
								ennuplaRisultatoRadice.add(entry.getKey(), entry.getValue());
						}
					}
				}
				else 
					if (!nodo.get(0).equals(radice.get(0)) && nodo.contains(tabellaRisultati)){ //un risultato per più ennuple 
					for(int j=0; j<mappaRisultati.get(nodo).size(); j++){
						JsonObject ennuplaRisultatoNodo = mappaRisultati.get(nodo).get(j).getAsJsonObject();
						for(int i=0; i<size; i++){
							JsonObject ennuplaRisultatoRadice = mappaRisultati.get(radice).get(i).getAsJsonObject();
							String chiaveEnnuplaRadice = ennuplaRisultatoRadice.get(nodo.get(0)+"_id").getAsString().replaceAll(Pattern.quote("\""), "");
							String chiaveEnnuplaNodo = ennuplaRisultatoNodo.get(nodo.get(0)+"_id").getAsString().replaceAll(Pattern.quote("\""), "");
							if(chiaveEnnuplaRadice.equals(chiaveEnnuplaNodo)){
								for(Entry<String, JsonElement> entry :ennuplaRisultatoNodo.entrySet()){
									if(mappaSelect.get(tabellaRisultati).get(0).equals("*") || mappaSelect.get(tabellaRisultati).contains(tabellaRisultati+"."+entry.getKey())) //aggiunge solo i campi richiesti
										ennuplaRisultatoRadice.add(entry.getKey(), entry.getValue());
								}
							}
						}
					}
				}
			}
		}
		if(!listaProiezioni.isEmpty() && !listaProiezioni.contains("*")){
			List<String> listaCampiDaProiettare = new LinkedList<>();
			List<String> campiDaEliminare = new LinkedList<>();
			for(String proiezione : listaProiezioni)
				listaCampiDaProiettare.add(proiezione.split("\\.")[1]);
			for(JsonElement je : mappaRisultati.get(radice)){ //trovo i campi da eliminare
				JsonObject ennuplaRisultatoFinale = je.getAsJsonObject();
				for(Entry<String, JsonElement> entry :ennuplaRisultatoFinale.entrySet()){
					if(!listaCampiDaProiettare.contains(entry.getKey()) && !campiDaEliminare.contains(entry.getKey()))
						campiDaEliminare.add(entry.getKey());
				}
			}
			for(JsonElement je : mappaRisultati.get(radice)){ // rimuovo i campi selezionati
				JsonObject ennuplaRisultatoFinale = je.getAsJsonObject();
				for(String campo : campiDaEliminare)
					ennuplaRisultatoFinale.remove(campo);
			}
		}
		return mappaRisultati.get(radice);
	}


	public void run(String query) throws Exception {

		QueryParser parser = this.getParser(query);	
		parser.spezza(query);
		FabbricatoreMappaStatement fabbricatoreMappe = new FabbricatoreMappaStatement();
		List<String> listaProiezioni = parser.getListaProiezioni();
		List<String> listaTabelle = parser.getListaTabelle();//tabelle che formano la query
		List<List<String>> matriceWhere = parser.getMatriceWhere();
		CaricatoreJson caricatoreJson = new CaricatoreJson();
		Map<String, JsonObject> jsonUtili = caricatoreJson.caricaJSON(listaTabelle);
		System.out.println("json scaricati: \n" + jsonUtili + "\n");
		Map<String, List<String>> mappaSelect = fabbricatoreMappe.creaMappaSelect(listaProiezioni, jsonUtili);
		System.out.println("lista proiezioni = "+listaProiezioni.toString());


		Map<String, List<List<String>>> mappaWhere = fabbricatoreMappe.creaMappaWhere(matriceWhere, jsonUtili);
		System.out.println("mappaWhere :"+ mappaWhere.toString()+"\n");
		
		Map<String, List<String>> mappaDB = fabbricatoreMappe.getMappaDB(jsonUtili);
		System.out.println("Mappa DB :"+mappaDB.toString());
		
		FabbricatoreAlberoEsecuzione fabbricatoreAlberoEsecuzione = new FabbricatoreAlberoEsecuzione();
		SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita = fabbricatoreAlberoEsecuzione.getGrafoPriorita(listaTabelle, mappaWhere); //non pesato per fare testing
		System.out.println("Grafo Priorità :" + grafoPriorita.toString());


		SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto = fabbricatoreAlberoEsecuzione.getGrafoPrioritaCompatto(grafoPriorita, jsonUtili, mappaDB);
		System.out.println("Grafo Priorità Compatto :"+grafoPrioritaCompatto.toString());

		SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoCopia = fabbricatoreAlberoEsecuzione.copiaGrafo(grafoPrioritaCompatto);
		
		List<String> radice = fabbricatoreAlberoEsecuzione.getRadice(grafoPrioritaCompatto);


		Map<List<String>, JsonArray> mappaRisultati = new HashMap<>();

		WorkflowManager workflowManager = new WorkflowManager();
		workflowManager.esegui(grafoPrioritaCompatto, grafoCopia, grafoPriorita, jsonUtili, mappaWhere, mappaSelect, mappaRisultati);
		System.out.println("MAPPA RISULTATI PRIMA DELLE PROIEZIONI = "+mappaRisultati.toString());
		workflowManager.eseguiProiezioni(grafoPrioritaCompatto, mappaSelect, mappaRisultati, mappaDB, mappaWhere);
		System.out.println("MAPPA RISULTATI DOPO LE PROIEZIONI = "+mappaRisultati.toString());
		JsonArray risultato = this.effettuaJoinRisultatoFinale(mappaRisultati, mappaSelect, radice, listaProiezioni); //metodo che unisce i jsonArray nella mappaRisultati
		System.out.println("\n\nRISULTATO FINALE =\n"+risultato.toString());
	}
}