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

	private static final String PATH_JSON_UTILI = "/Users/leorossi/Desktop/fileJSON.txt";

	/**
	 * Carica da file i json utili in base alle tabelle
	 * @param listaFrom Tabelle di partenza
	 * @param filePath Path del file con le indicazioni necessarie
	 * 
	 * @throws FileNotFoundException
	 */
	public Map<String, JsonObject> caricaJSON(List<String> listaFrom, String filePath) throws FileNotFoundException{
		Map<String,JsonObject> jsonCheMiServono = new HashMap<String, JsonObject>();
		File fileJSON = new File(filePath);
		Scanner scanner = new Scanner(fileJSON);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			for(String tabella : listaFrom){			
				JsonParser parser = new JsonParser();
				JsonObject myJson = parser.parse(line).getAsJsonObject();
				String table = myJson.get("table").getAsString();
				if(table.equals(tabella)){
					jsonCheMiServono.put(tabella, myJson);
				}
			}
		}
		scanner.close();
		return jsonCheMiServono;
	}


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

	private SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> getGrafoPriorita(List<String> tabelle, Map<String, List<List<String>>> mappaWhere) {
		SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
		for(int i=0; i<tabelle.size(); i++){
			String tabellaCorrente = tabelle.get(i);
			grafoPriorita.addVertex(tabellaCorrente);
			List<String> figli = getFigli(tabellaCorrente, mappaWhere, tabelle);
			for(int j=0; j<figli.size(); j++){
				String figlio = figli.get(j);
				grafoPriorita.addVertex(figlio);
				DefaultWeightedEdge e = grafoPriorita.addEdge(tabellaCorrente, figlio);
				grafoPriorita.setEdgeWeight(e, 0);
			}
		}
		return grafoPriorita;
	}

	private static List<String> getFigli(String tabella, Map<String, List<List<String>>> mappaWhere, List<String> tabelle){
		List<String> figli = new LinkedList<>();
		List<List<String>> condizioniTabella = mappaWhere.get(tabella);
		for(int i=0; i<condizioniTabella.size(); i++){
			List<String> condizione = condizioniTabella.get(i);
			if(tabelle.contains(condizione.get(1).split("\\.")[0]))
				figli.add(condizione.get(1).split("\\.")[0]);
		}
		return figli;
	}

	private static boolean allEquals(List<String> tabelle) {
		boolean allEquals = true;
		for (String s : tabelle) {
			if(!s.equals(tabelle.get(0)))
				allEquals = false;
		}
		return allEquals;
	}

	private SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> getGrafoPrioritaCompatto(SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita, Map<String, JsonObject> jsonUtili, Map<String, List<String>> mappaDB){
		SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
		List<DefaultWeightedEdge> archiUtili = new LinkedList<>();
		for(String db : mappaDB.keySet()){
			List<String> nodi = mappaDB.get(db);
			Iterator<String> i = nodi.iterator();
			while(i.hasNext()){
				String nodoCorrente = i.next();
				Iterator<DefaultWeightedEdge> iArchi = grafoPriorita.incomingEdgesOf(nodoCorrente).iterator();
				System.out.println("nodo corrente :"+nodoCorrente);
				if(iArchi.hasNext()){
					DefaultWeightedEdge arcoEntrante = iArchi.next();
					String padre = grafoPriorita.getEdgeSource(arcoEntrante);
					if(!jsonUtili.get(padre).get("database").getAsString().equals(db)){
						List<String> nodoNuovoGrafo = new LinkedList<>();
						nodoNuovoGrafo.add(nodoCorrente);
						archiUtili.add(arcoEntrante);
						System.out.println("archi utili :"+archiUtili);
						addDiscendentiStessoDB(nodoNuovoGrafo, nodoCorrente, db, grafoPriorita, jsonUtili);
						grafoPrioritaCompatto.addVertex(nodoNuovoGrafo);
					}
				}
				else{ //caso in cui ci stiamo occupando della radice 
					List<String> nodoNuovoGrafo = new LinkedList<>();
					nodoNuovoGrafo.add(nodoCorrente);
					addDiscendentiStessoDB(nodoNuovoGrafo, nodoCorrente, db, grafoPriorita, jsonUtili);
					grafoPrioritaCompatto.addVertex(nodoNuovoGrafo);
				}
			}
			System.out.println("GRAFO PARZIALE:"+grafoPrioritaCompatto.toString());
		}
		//aggiungo gli archi
		for(DefaultWeightedEdge e : archiUtili){
			System.out.println("Arco Da Aggiungere:"+ e);
			for(List<String> nodoPadre : grafoPrioritaCompatto.vertexSet()){
				if(nodoPadre.contains(grafoPriorita.getEdgeSource(e))){
					System.out.println("nodoPadre = "+nodoPadre+"\n Contains = "+grafoPriorita.getEdgeSource(e));
					for(List<String> nodoFiglio : grafoPrioritaCompatto.vertexSet()){
						if(nodoFiglio.contains(grafoPriorita.getEdgeTarget(e))){
							System.out.println("nodoFiglio = "+nodoFiglio+"\n Contains = "+grafoPriorita.getEdgeTarget(e));
							grafoPrioritaCompatto.addEdge(nodoPadre, nodoFiglio);

						}
					}
				}	
			}
		}
		return grafoPrioritaCompatto;
	}

	private static void addDiscendentiStessoDB(List<String> nodoNuovoGrafo, String nodoCorrente, String db, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita, Map<String, JsonObject> jsonUtili) {
		Set<DefaultWeightedEdge> archi = grafoPriorita.outgoingEdgesOf(nodoCorrente);
		if(archi==null)
			return;
		else{
			Iterator<DefaultWeightedEdge> i = archi.iterator();
			while(i.hasNext()){
				DefaultWeightedEdge arco = i.next();
				String figlio = grafoPriorita.getEdgeTarget(arco);
				if(jsonUtili.get(figlio).get("database").getAsString().equals(db)){
					nodoNuovoGrafo.add(figlio);
					addDiscendentiStessoDB(nodoNuovoGrafo, figlio, db, grafoPriorita, jsonUtili);
				}
			}
		}
	}

	private Map<String, List<String>> getMappaDB(Map<String, JsonObject> jsonUtili){
		Map<String, List<String>> mappaDB = new HashMap<>();
		for(String k : jsonUtili.keySet()){
			String database = jsonUtili.get(k).get("database").getAsString();
			if(!mappaDB.containsKey(database))
				mappaDB.put(database, new LinkedList<String>());
			mappaDB.get(database).add(jsonUtili.get(k).get("table").getAsString());
		}
		return mappaDB;
	}

	private SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> copiaGrafo (SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPriorita){
		SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> copia = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
		for(List<String> nodo : grafoPriorita.vertexSet()){
			List<String> nuovoNodo = new LinkedList<>(nodo);
			copia.addVertex(nuovoNodo);
		}
		for(DefaultWeightedEdge arco : grafoPriorita.edgeSet()){
			copia.addEdge(grafoPriorita.getEdgeSource(arco), grafoPriorita.getEdgeTarget(arco));
		}
		return copia;
	}

	/**
	 * 
	 * @param query La query da parsare
	 * @return Il giusto parser per la query data
	 * @throws MalformedQueryException
	 */
	private QueryParser identificatoreQuery(String query) throws MalformedQueryException	{
		if (query.toLowerCase().startsWith("select"))
			return new ParserSql();
		else if (query.toLowerCase().startsWith("match"))
			return new ParserNeo4j();
		else if (query.toLowerCase().startsWith("db."))
			return new ParserMongo();
		throw new MalformedQueryException("La query in input non è SQL, Cypher o Mongo");

	}

	private QueryParser parseQuery(String query) throws Exception {
		QueryParser parser = this.identificatoreQuery(query);
		parser.spezza(query);
		return parser;
	}

	private JsonArray effettuaJoinRisultatoFinale(Map<List<String>, JsonArray> mappaRisultati,
			Map<String, List<String>> mappaSelect, List<String> radice) {
		int size = mappaRisultati.get(radice).size();
		for(String tabellaRisultati : mappaSelect.keySet()){
			for(List<String> nodo : mappaRisultati.keySet()){
				if(nodo.contains(tabellaRisultati) && mappaRisultati.get(nodo).size()==size){ //aggiungo uno ad uno
					for(int i=0; i<size; i++){
						JsonObject risultatoRadice = mappaRisultati.get(radice).get(i).getAsJsonObject();
						JsonObject risultatoNodo = mappaRisultati.get(nodo).get(i).getAsJsonObject();
						for(Entry<String, JsonElement> entry :risultatoNodo.entrySet()){
							if(mappaSelect.get(tabellaRisultati).contains(tabellaRisultati+"."+entry.getKey())) //aggiunge solo i campi richiesti
								risultatoRadice.add(entry.getKey(), entry.getValue());
						}
					}
				}
				else if (nodo.contains(tabellaRisultati) && mappaRisultati.get(nodo).size()==1){ //un risultato per tutte le ennupla 
					JsonObject risultatoNodo = mappaRisultati.get(nodo).get(0).getAsJsonObject();
					for(int i=0; i<size; i++){
						JsonObject risultatoRadice = mappaRisultati.get(radice).get(i).getAsJsonObject();
						for(Entry<String, JsonElement> entry :risultatoNodo.entrySet()){
							if(mappaSelect.get(tabellaRisultati).contains(tabellaRisultati+"."+entry.getKey()))
								risultatoRadice.add(entry.getKey(), entry.getValue());
						}
					}
				}
			}
		}
		return mappaRisultati.get(radice);
	}


	public void run(String query) throws Exception {

		QueryParser parser = this.parseQuery(query);		
		FabbricatoreMappaStatement fabbricatoreMappe = new FabbricatoreMappaStatement();
		List<String> listaProiezioni = parser.getListaProiezioni();
		Map<String, List<String>> mappaSelect = fabbricatoreMappe.creaMappaSelect(listaProiezioni);
		System.out.println("lista proiezioni = "+listaProiezioni.toString());
		List<String> tabelle = parser.getListaTabelle();//tabelle che formano la query
		List<List<String>> matriceWhere = parser.getMatriceWhere();
		Map<String, JsonObject> jsonUtili = this.caricaJSON(tabelle,PATH_JSON_UTILI);
		System.out.println("json scaricati: \n" + jsonUtili + "\n");

		Map<String, List<List<String>>> mappaWhere = fabbricatoreMappe.creaMappaWhere(matriceWhere, jsonUtili);
		System.out.println("mappaWhere :"+ mappaWhere.toString()+"\n");

		SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita = this.getGrafoPriorita(tabelle, mappaWhere); //non pesato per fare testing
		System.out.println("Grafo Priorità :" + grafoPriorita.toString());

		Map<String, List<String>> mappaDB = this.getMappaDB(jsonUtili);
		System.out.println("Mappa DB :"+mappaDB.toString());

		SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto = this.getGrafoPrioritaCompatto(grafoPriorita, jsonUtili, mappaDB);
		System.out.println("Grafo Priorità Compatto :"+grafoPrioritaCompatto.toString());

		SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoCopia = this.copiaGrafo(grafoPrioritaCompatto);

		Map<List<String>, JsonArray> mappaRisultati = new HashMap<>();

		GestoreQuery gestoreQuery = new GestoreQuery();
		gestoreQuery.esegui(grafoPrioritaCompatto, grafoCopia, grafoPriorita, jsonUtili, mappaWhere, mappaSelect, mappaRisultati);
		System.out.println("MAPPA RISULTATI PRIMA DELLE PROIEZIONI = "+mappaRisultati.toString());
		gestoreQuery.eseguiProiezioni(grafoPrioritaCompatto, mappaSelect, mappaRisultati, mappaDB, mappaWhere);
		System.out.println("MAPPA RISULTATI DOPO LE PROIEZIONI = "+mappaRisultati.toString());
		JsonArray risultato = this.effettuaJoinRisultatoFinale(mappaRisultati, mappaSelect,  gestoreQuery.getRadice(grafoPrioritaCompatto)); //metodo che unisce i jsonArray nella mappaRisultati
		System.out.println("\n\nRISULTATO FINALE =\n"+risultato.toString());
	}

	public static void main(String[] argc) {
		//		String query = "SELECT film.title, customer.last_name FROM customer, rental, inventory, film WHERE rental.customer_id = customer.customer_id AND rental.inventory_id = inventory.inventory_id AND inventory.film_id = film.film_id AND customer.last_name = 'Bianchi'"; 
		//		String query = "MATCH (a:address), (s:store) WHERE a.address_id = s.address_id AND s.address_id = 4";
		String query = "SELECT store.address_id FROM payment, rental, inventory, film, store, customer, address, city, country WHERE payment.rental_id = rental.rental_id AND rental.customer_id = customer.customer_id AND rental.inventory_id = inventory.inventory_id AND inventory.film_id = film.film_id AND inventory.store_id = store.store_id AND customer.address_id = address.address_id AND address.city_id = city.city_id AND city.country_id = country.country_id";
		try {
			new Polystore().run(query);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

}