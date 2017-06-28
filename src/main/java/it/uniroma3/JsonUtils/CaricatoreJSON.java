package it.uniroma3.JsonUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.StringTokenizer;

import javax.swing.tree.*;

import org.jgrapht.WeightedGraph;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.jgrapht.graph.SimpleWeightedGraph;
import org.jgrapht.graph.builder.DirectedWeightedGraphBuilder;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import it.uniroma3.JsonUtils.parser.ParserMongo;
import it.uniroma3.JsonUtils.parser.ParserNeo4j;
import it.uniroma3.JsonUtils.parser.ParserSql;
import it.uniroma3.JsonUtils.parser.QueryParser;
import it.uniroma3.exeptions.MalformedQueryException;
import net.sf.jsqlparser.JSQLParserException;
import scala.reflect.internal.Trees.This;

public class CaricatoreJSON {

	private static final String PATH_JSON_UTILI = /Users/leorossi/Desktop/fileJSON.txt;

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
		//String tabellaPreferita = tabelle.get(0);
		//System.out.println("TABELLE :" + tabelle);
		if (tabelle.size()==1)
			return tabelle;
		List<String> tabellePreferite = new LinkedList<>();
		if (allEquals(tabelle) == true){
			tabellePreferite.add(tabelle.get(0));		
		}
		//tabellePreferite.add(tabellaPreferita);
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

	public static String getRadice(List<String> tabellePriorità){
		return tabellePriorità.get(0);
	}

	public static boolean isStarQuery(String radice, List<String> tabelle, Map<String, List<List<String>>> mappaWhere){
		int contatoreJoin = 0;
		List<List<String>> condizioniRadice = mappaWhere.get(radice);
		for(int i=0; i<condizioniRadice.size(); i++){
			List<String> condizione = condizioniRadice.get(i);
			for(int j=0; j<tabelle.size(); j++){
				String tabella = tabelle.get(j);
				if(condizione.get(1).split("\\.")[0].equals(tabella)){
					System.out.println("JOIN "+radice+" con "+tabella);
					contatoreJoin++;
				}
			}
		}
		return contatoreJoin>=2;		
	}

	private static List<String> rimuovi(List<String> lista, String elemento){
		List<String> risultato = new LinkedList<>(lista);
		risultato.remove(elemento);
		return risultato;		
	}

	//	private static <K, V> Map<K,V> rimuovi(Map<K,V> mappa, String elemento){
	//		Map<K,V> risultato = new HashMap<>(mappa);
	//		risultato.remove(elemento);
	//		return risultato;		
	//	}


	//	public static List<List<String>> getMatricePriorita(List<String> tabelle, Map<String, JsonObject> jsonUtili,Map<String, List<List<String>>> mappaWhere, List<List<String>> matricePriorita){ 
	//		System.out.println("\n"+ "Matrice Priorità parziale :"+ matricePriorita);
	//		if(tabelle.isEmpty())
	//			return matricePriorita;
	//		else{
	//			List<String> rigaDaAggiungere = new LinkedList<>();
	//			String radice = getRadice(getTabellaPrioritaAlta(tabelle, jsonUtili));
	//			System.out.println("Radice sottoalbero : "+ radice);
	//			List<String> tabelleModificate = rimuovi(tabelle, radice); 
	//			
	//			if(isStarQuery(radice, tabelle, mappaWhere)){
	//				System.out.println("isStarQuery:"+ radice);
	//				List<String> rigaSoloRadice = new LinkedList<>(); //Intanto aggiungo l'elemento radice che viene sempre rimosso da jsonUtili, tabelle e mappaWhere
	//				rigaSoloRadice.add(radice);
	//				matricePriorita.add(rigaSoloRadice);
	//				
	//				List<List<String>> condizioniRadice = mappaWhere.get(radice);
	//				for(int i=0; i<condizioniRadice.size(); i++){
	//					List<String> condizione = condizioniRadice.get(i);
	//					for(int j=0; j<tabelle.size(); j++){
	//						String tabella = tabelle.get(j);
	//						if(condizione.get(1).split("\\.")[0].equals(tabella)){
	//							rigaDaAggiungere.add(tabella);
	//							tabelleModificate = rimuovi(tabelleModificate, tabella);
	//						}
	//					}
	//				}
	//				matricePriorita.add(rigaDaAggiungere);
	//				return getMatricePriorita(tabelleModificate, jsonUtili, mappaWhere, matricePriorita); 
	//			}else{ //Se non è una query a stella
	//				rigaDaAggiungere.add(radice);
	//				matricePriorita.add(rigaDaAggiungere);
	//				return getMatricePriorita(tabelleModificate, jsonUtili, mappaWhere, matricePriorita); //jsonUtiliModificati e mappaWhereModificata
	//			}
	//		}
	//	}

	public static SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> getAlberoPriorita(List<String> tabelle, Map<String, JsonObject> jsonUtili, Map<String, List<List<String>>> mappaWhere, Map<String, Integer> mappaWeight){
		SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> alberoPriorita = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
		for(int i=0; i<tabelle.size(); i++){
			String tabellaCorrente = tabelle.get(i);
			alberoPriorita.addVertex(tabellaCorrente);
			List<String> figli = getFigli(tabellaCorrente, mappaWhere, tabelle);
			for(int j=0; j<figli.size(); j++){
				String figlio = figli.get(j);
				alberoPriorita.addVertex(figlio);
				DefaultWeightedEdge e = alberoPriorita.addEdge(tabellaCorrente, figlio);
				alberoPriorita.setEdgeWeight(e, getJoinWeight(tabellaCorrente, figlio, mappaWeight, mappaWhere, jsonUtili));
			}
		}
		return alberoPriorita;
	}



	public SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> getAlberoPriorita(List<String> tabelle,
			Map<String, JsonObject> jsonUtili, Map<String, List<List<String>>> mappaWhere) {
		SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> alberoPriorita = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
		for(int i=0; i<tabelle.size(); i++){
			String tabellaCorrente = tabelle.get(i);
			alberoPriorita.addVertex(tabellaCorrente);
			List<String> figli = getFigli(tabellaCorrente, mappaWhere, tabelle);
			for(int j=0; j<figli.size(); j++){
				String figlio = figli.get(j);
				alberoPriorita.addVertex(figlio);
				DefaultWeightedEdge e = alberoPriorita.addEdge(tabellaCorrente, figlio);
				alberoPriorita.setEdgeWeight(e, 0);
			}
		}
		return alberoPriorita;
	}



	public void aggiornaWeights(SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita, Map<String, JsonArray> mappaRisultati, Map<String, Integer> mappaWeight, String foglia, Map<String, List<List<String>>> mappaWhere, Map<String, JsonObject> jsonUtili){
		mappaWeight.put(foglia,  mappaRisultati.get(foglia).size());
		for(DefaultWeightedEdge e :grafoPriorita.incomingEdgesOf(foglia)) //sarà solo 1
			grafoPriorita.setEdgeWeight(e, getJoinWeight(grafoPriorita.getEdgeSource(e), foglia, mappaWeight, mappaWhere, jsonUtili));
	}



	public static int getJoinWeight(String padre, String figlio, Map<String, Integer> mappaWeight,Map<String, List<List<String>>> mappaWhere, Map<String, JsonObject> jsonUtili){
		int weight = 0;
		for(int i=0; i<mappaWhere.get(padre).size(); i++){
			List<String> condizione = mappaWhere.get(padre).get(i);
			String parametroJoin = condizione.get(1);
			if(parametroJoin.split("\\.")[0].equals(figlio)){
				if(parametroJoin.equals(jsonUtili.get(figlio).getAsJsonObject().get("primarykey")))
					weight = mappaWeight.get(padre);
				else
					weight = mappaWeight.get(padre)*mappaWeight.get(figlio);
			}
		}
		return weight;
	}




	public static List<String> getFigli(String tabella, Map<String, List<List<String>>> mappaWhere, List<String> tabelle){
		List<String> figli = new LinkedList<>();
		List<List<String>> condizioniTabella = mappaWhere.get(tabella);
		for(int i=0; i<condizioniTabella.size(); i++){
			List<String> condizione = condizioniTabella.get(i);
			if(tabelle.contains(condizione.get(1).split("\\.")[0]))
				figli.add(condizione.get(1).split("\\.")[0]);
		}
		return figli;
	}

	//DA LEVARE MAPPAWHERE E GLI ALTRI PARAMETRI INUTILI CHE VERRANNO CAMBIATI UNA VOLTA CHE CAMBIERA' GESTOREQUERY
	private static Map<String, Integer> getMappaWeight(List<String> tabelle, Map<String, JsonObject> jsonUtili, GestoreQuery gestoreQuery, Map<String, List<List<String>>> mappaWhere) throws Exception {
		Map<String, Integer> mappaWeight = new HashMap<>();
		for(String tabella : tabelle){
			String query;
			if(jsonUtili.get(tabella).get("database").equals("postgreSQL"))
				query = "SELECT COUNT (*) FROM "+tabella;
			if(jsonUtili.get(tabella).get("database").equals("neo4j"))
				query = "START n=node(*) MATCH (n:"+tabella+") RETURN count(n)";
			if(jsonUtili.get(tabella).get("database").equals("mongoDB"))
				query = "db."+tabella+".count()";
			JsonArray results = gestoreQuery.esegui(jsonUtili.get(tabella), null, jsonUtili, mappaWhere); //magari fai anche un metodo a parte solo per i count dentro GestoreQuery per farlo più veloce
			int weight = results.get(0).getAsInt();                                                       //anche perchè la query non la fa con count da solo
			mappaWeight.put(tabella, weight);
		}
		return mappaWeight;
	}


	private static boolean allEquals(List<String> tabelle) {
		boolean allEqual = true;
		for (String s : tabelle) {
			if(!s.equals(tabelle.get(0)))
				allEqual = false;
		}
		return allEqual;
	}

	public SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> getGrafoPrioritaCompatto(SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita, Map<String, JsonObject> jsonUtili, Map<String, List<String>> mappaDB){
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

	public SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> copiaGrafo (SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPriorita){
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

	public QueryParser identificatoreQuery(String query) throws MalformedQueryException	{
		if (query.toLowerCase().startsWith("select"))
			return new ParserSql();
		else if (query.toLowerCase().startsWith("match"))
			return new ParserNeo4j();
		else if (query.toLowerCase().startsWith("db."))
			return new ParserMongo();
		throw new MalformedQueryException("La query in input non è SLQ, Cypher o Mongo");

	}

	private QueryParser parseQuery(String query) throws Exception {
		QueryParser parser = this.identificatoreQuery(query);
		parser.spezza(query);
		return parser;
	}
	
	public void run(String query) throws Exception {

		QueryParser parser = this.parseQuery(query);		
		List<String> tabelle = parser.getTableList();//tabelle che formano la query
		List<List<String>> matriceWhere = parser.getMatriceWhere();
		Map<String, JsonObject> jsonUtili = this.caricaJSON(tabelle,PATH_JSON_UTILI);
		System.out.println("json scaricati: \n" + jsonUtili + "\n");

		FabbricatoreMappaStatement fabbricatoreCondizione = new FabbricatoreMappaStatement();
		fabbricatoreCondizione.creaMappaWhere(matriceWhere, jsonUtili);
		Map<String, List<List<String>>> mappaWhere = fabbricatoreCondizione.getMappaWhere();
		System.out.println("mappaWhere :"+ mappaWhere.toString()+"\n");

		SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita = this.getAlberoPriorita(tabelle, jsonUtili, mappaWhere); //non pesato per fare testing
		System.out.println("Grafo Priorità :" + grafoPriorita.toString());

		Map<String, List<String>> mappaDB = this.getMappaDB(jsonUtili);
		System.out.println("Mappa DB :"+mappaDB.toString());

		SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto = this.getGrafoPrioritaCompatto(grafoPriorita, jsonUtili, mappaDB);
		System.out.println("Grafo Priorità Compatto :"+grafoPrioritaCompatto.toString());

		SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoCopia = this.copiaGrafo(grafoPrioritaCompatto);

		new GestoreQuery().esegui(grafoPrioritaCompatto, grafoCopia, grafoPriorita, jsonUtili, mappaWhere);
	}

	public static void main (String[] args) throws Exception {
		String query = "SELECT * FROM address, store WHERE address.address_id = store.address_id AND store.address_id = 4"; 
		//String query = "MATCH (a:address), (s:store) WHERE a.address_id = s.address_id AND s.address_id = 4";
		new CaricatoreJSON().run(query);
	}
}