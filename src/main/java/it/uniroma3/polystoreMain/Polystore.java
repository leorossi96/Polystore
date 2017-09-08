package it.uniroma3.polystoreMain;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import it.uniroma3.exeptions.MalformedQueryException;
import it.uniroma3.grafiPriotita.CaricatoreJson;
import it.uniroma3.grafiPriotita.FabbricatoreAlberoEsecuzione;
import it.uniroma3.grafiPriotita.FabbricatoreMappaStatement;
import it.uniroma3.json.AggregatoreJson;
import it.uniroma3.json.JsonWriter;
import it.uniroma3.queryParser.ParserMongo;
import it.uniroma3.queryParser.ParserNeo4j;
import it.uniroma3.queryParser.ParserSql;
import it.uniroma3.queryParser.QueryParser;

public class Polystore {

	private static final String properties = "percorsoRisultato.properties";

	private String PATH;

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

	private org.json.simple.JSONArray effettuaJoinRisultatoFinale(Map<List<String>, JsonArray> mappaRisultati, Map<String, List<String>> mappaSelect, Map<String, JsonObject> jsonUtili) throws IOException, ParseException {
//		PATH = percorsoFileRisultato();
		List<String> paths = new LinkedList<>();
		JsonWriter writer = new JsonWriter();
		List<String> requiredColumns = new LinkedList<>();
		List<List<String>> nodiRisultato = new LinkedList<>();
		for(String tabellaProiezione : mappaSelect.keySet()){
			if(!tabellaProiezione.equals("*") && mappaSelect.get(tabellaProiezione).get(0).equals("*")){
				JsonArray membri = jsonUtili.get(tabellaProiezione).getAsJsonArray("members");
				for(JsonElement membro : membri){
					requiredColumns.add(membro.getAsString().replaceAll("\"", "").split("\\.")[0]);
				}
			}else if(!tabellaProiezione.equals("*"))
				requiredColumns.addAll(mappaSelect.get(tabellaProiezione));
			for(List<String> nodo : mappaRisultati.keySet()) {
				if(nodo.contains(tabellaProiezione) && !nodiRisultato.contains(nodo)){
					
					JsonArray array = mappaRisultati.get(nodo);
					if (array.size()!=0){
						nodiRisultato.add(nodo);
						//						System.out.println("PROVIENE DA: "+nodo.toString());
						String path = writer.writeArrayTemporary(array);
						paths.add(path);
					}
				}
			}
		}
		if(paths.size() == 1) {

//			/*Nel caso di un solo risultato non c'è bisogno di invocare AggregatoreJson*/
//			Path source = Paths.get(paths.get(0));
//			File target = new File(PATH + "/risultati.json"); //TODO rendere parametrico insieme a quello di spark
//			if(!target.exists())
//				target.createNewFile();
//			OutputStream fos = new FileOutputStream(target);
//			Files.copy(source, fos);
			JSONParser parser = new JSONParser();
			org.json.simple.JSONArray jsonObject = (org.json.simple.JSONArray) parser.parse(new FileReader(paths.get(0)));
			return jsonObject;
		}
//		System.out.println("OOOOOOOOOOOOOOOOOOOOOOOOOOOO\n"+requiredColumns);
//		AggregatoreJson aggregatore = new AggregatoreJson();
//		aggregatore.join(paths, requiredColumns);
		return null;
	}

//	private String percorsoFileRisultato() throws IOException{
//		ClassLoader loader = Thread.currentThread().getContextClassLoader();
//		Properties props = new Properties();
//		InputStream resource = loader.getResourceAsStream(properties);
//		props.load(resource);
//		return props.getProperty("PATH");
//	}

	public org.json.simple.JSONArray run(String query) throws Exception {

		QueryParser parser = new ParserSql();//this.getParser(query);	
		parser.spezza(query);
		FabbricatoreMappaStatement fabbricatoreMappe = new FabbricatoreMappaStatement();
		List<String> listaProiezioni = parser.getListaProiezioni();
		List<String> listaTabelle = parser.getListaTabelle();//tabelle che formano la query
		List<List<String>> matriceWhere = parser.getMatriceWhere();
		CaricatoreJson caricatoreJson = new CaricatoreJson();
		Map<String, JsonObject> jsonUtili = caricatoreJson.caricaJSON(listaTabelle);
		Map<String, List<String>> mappaSelect = fabbricatoreMappe.creaMappaSelect(listaProiezioni, jsonUtili);
//		System.out.println("MAPPA SELECT ="+mappaSelect.toString());
//		System.out.println("lista proiezioni = "+listaProiezioni.toString());

		Map<String, List<List<String>>> mappaWhere = fabbricatoreMappe.creaMappaWhere(matriceWhere, jsonUtili);
//		System.out.println("mappaWhere :"+ mappaWhere.toString()+"\n");

		Map<String, List<String>> mappaDB = fabbricatoreMappe.getMappaDB(jsonUtili);
//		System.out.println("Mappa DB :"+mappaDB.toString());

		FabbricatoreAlberoEsecuzione fabbricatoreAlberoEsecuzione = new FabbricatoreAlberoEsecuzione();
		SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita = fabbricatoreAlberoEsecuzione.getGrafoPriorita(listaTabelle, mappaWhere); //non pesato per fare testing
//		System.out.println("Grafo Priorità :" + grafoPriorita.toString());


		SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto = fabbricatoreAlberoEsecuzione.getGrafoPrioritaCompatto(grafoPriorita, jsonUtili, mappaDB);
//		System.out.println("Grafo Priorità Compatto :"+grafoPrioritaCompatto.toString());

		SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoCopia = fabbricatoreAlberoEsecuzione.copiaGrafo(grafoPrioritaCompatto);

		Map<List<String>, JsonArray> mappaRisultati = new HashMap<>();

		WorkflowManager workflowManager = new WorkflowManager();
		workflowManager.esegui(grafoPrioritaCompatto, grafoCopia, grafoPriorita, jsonUtili, mappaWhere, mappaSelect, mappaRisultati);
//		System.out.println("FINITO");
		workflowManager.eseguiProiezioni(grafoPrioritaCompatto, mappaSelect, mappaRisultati, mappaDB, mappaWhere, jsonUtili);
		final long startTime = System.currentTimeMillis();
		return this.effettuaJoinRisultatoFinale(mappaRisultati, mappaSelect, jsonUtili);
//		final long elapsedTime = System.currentTimeMillis() - startTime;
//		System.out.println("TEMPO AGGREGAZIONE = "+ elapsedTime/ 1000.0);
	}

	public static void main (String[]args) throws Exception{
		//String query = "SELECT * FROM moviecredits, credits WHERE moviecredits.id_credit = credits.id_credit";
		//String query = "SELECT * FROM moviecredits, movies WHERE moviecredits.id_movie = movies.id_movie AND movies.id_movie = '141423'";
		String query = "SELECT * FROM movies, moviecredits, credits WHERE moviecredits.id_movie = movies.id_movie AND moviecredits.id_credit = credits.id_credit AND movies.id_movie = '141423'";
		//String query = "SELECT * FROM moviecredits, credits WHERE moviecredits.id_credit = credits.id_credit AND moviecredits.id_movie = '141423'";
		new Polystore().run(query);

	}
	
	public org.json.simple.JSONArray executeQuery(String query) throws Exception {
		return this.run(query);
	}

	private static String lettoreQuery() {
	
		return null;
	}
}
