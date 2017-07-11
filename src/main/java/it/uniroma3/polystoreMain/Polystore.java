package it.uniroma3.polystoreMain;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import it.uniroma3.exeptions.MalformedQueryException;
import it.uniroma3.grafiPriotita.CaricatoreJson;
import it.uniroma3.grafiPriotita.FabbricatoreAlberoEsecuzione;
import it.uniroma3.grafiPriotita.FabbricatoreMappaStatement;
import it.uniroma3.queryParser.ParserMongo;
import it.uniroma3.queryParser.ParserNeo4j;
import it.uniroma3.queryParser.ParserSql;
import it.uniroma3.queryParser.QueryParser;

public class Polystore {

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
		System.out.println("stampa");
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
		workflowManager.eseguiProiezioni(grafoPrioritaCompatto, mappaSelect, mappaRisultati, mappaDB, mappaWhere);
		System.out.println("avantiii");
		JsonArray risultato = this.effettuaJoinRisultatoFinale(mappaRisultati, mappaSelect, radice, listaProiezioni); //metodo che unisce i jsonArray nella mappaRisultati
		System.out.println("\n\nRISULTATO FINALE ="+risultato.toString()+"\n");
	}
	
	public static void main (String[]args) throws Exception{
//		String query = "SELECT customer.first_name, customer.last_name, rental.rental_id, inventory.inventory_id FROM inventory, rental, customer, address, city WHERE rental.inventory_id = inventory.inventory_id AND city.city = 'Lens' AND address.city_id = city.city_id AND rental.customer_id = customer.customer_id AND address.address_id = customer.address_id";
//		String query = "SELECT inventory.film_id, customer.address_id, address.address FROM rental, payment, customer, address, city, country, inventory WHERE inventory.inventory_id = rental.inventory_id AND rental.customer_id = customer.customer_id AND customer.address_id = address.address_id AND city.city_id = address.city_id AND rental.payment_id = payment.payment_id AND country.country_id = city.country_id";
//		String query = "SELECT * FROM language WHERE language.name = 'Tswana'";
//		String query = "SELECT city.city_id FROM city WHERE city.city = 'Lens'";
//		String query = "SELECT language.name FROM language WHERE language.name = 'Mongolian'";
//		String query = "SELECT * FROM category WHERE category.category_id = 100";
//		String query = "SELECT * FROM rental, payment WHERE rental.rental_id = payment.rental_id AND rental.inventory_id = 1";
//		String query = "SELECT rental.rental_id, payment.amount, customer.first_name, customer.last_name, film.title, store.store_id FROM payment, rental, customer, inventory, film, store, address, city, country WHERE payment.rental_id = rental.rental_id AND customer.customer_id = rental.customer_id AND rental.inventory_id = inventory.inventory_id AND inventory.film_id = film.film_id AND store.store_id = inventory.store_id AND customer.address_id = address.address_id AND address.city_id = city.city_id AND country.country_id = city.country_id";
//		String query = "SELECT customer.first_name, customer.last_name, address.address, address.address2 FROM customer, address WHERE customer.address_id = address.address_id";
//		String query = "SELECT customer.first_name, customer.last_name, payment.amount, address.address FROM rental, payment, customer, address WHERE rental.rental_id = payment.rental_id AND customer.customer_id = rental.customer_id AND customer.address_id = address.address_id";
		String query = "db.store.find({'store.store_id'= 1})";
//		String query = "MATCH (payment: payment)--(rental: rental) WHERE payment.rental_id = rental.rental_id AND payment.amount = 44.79 RETURN payment.payment_date";
//		String query = "MATCH (rental: rental)--(customer: customer)--(address: address) WHERE rental.customer_id = customer.customer_id AND customer.address_id = address.address_id";
		new Polystore().run(query);
	}
}
