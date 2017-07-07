package it.uniroma3.model;

import it.uniroma3.JsonUtils.CaricatoreJSON;
import it.uniroma3.JsonUtils.FabbricatoreMappaCondizioni;
import it.uniroma3.JsonUtils.GestoreQuery;
import it.uniroma3.JsonUtils.parser.ParserNeo4j;

import java.util.List;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Data la query Cypher iniziale, interroga tutte le tabelle e restituisce il risultato
 * @author micheletedesco1
 *
 */

/*
 * Questa classe funge da WorkflowManager data una query Cypher iniziale, interrogando le varie tabelle in cascata seguendo i collegamenti (join).
 * In pratica, data la tabella con priorità più alta, da questa partiro e navighero tutti i collegamenti richiesti (si veda la classe GestoreQuery).
 * ES: MATCH (customer:customer), (store:store) WHERE customer.store_id = store.id AND store.address_id = 1 RETURN customer.name
 * Parto dalla tabella customer------>store. Vedo che in customer è presente un join con store, quindi eseguo store : MATCH (store:store) WHERE store.address_id = 1 RETURN store.id
 * I risultati ottenuti andranno in customer: SELECT * FROM customer WHERE  customer.store_id = i risultati della query precedente.
 * @author micheletedesco1
 *
 */
public class FacadeCypher {
	public String gestisciQuery(String queryCypher) throws Exception{
		ParserNeo4j parser = new ParserNeo4j();
		parser.spezza(queryCypher);//spezzo la query
		List<String> tabelle = parser.getListaTabelle();//ottengo le tabelle che formano la query
		List<List<String>> matriceWhere = parser.getMatriceWhere();
		CaricatoreJSON caricatoreDAFile = new CaricatoreJSON();
		caricatoreDAFile.caricaJSON(tabelle);//carico da file i json utili in base alle tabelle
		Map<String, JsonObject> jsonUtili = caricatoreDAFile.getJsonCheMiServono();
		String tabellaPrioritàAlta = caricatoreDAFile.getTabellaPrioritaAlta();
		JsonObject questoJson = jsonUtili.get(tabellaPrioritàAlta); 
		FabbricatoreMappaCondizioni fabbricatoreCondizione = new FabbricatoreMappaCondizioni();
		fabbricatoreCondizione.creaMappaCondizioni(matriceWhere, jsonUtili);
		Map<String, List<List<String>>> mappaWhere = fabbricatoreCondizione.getMappaWhere();
		GestoreQuery gestoreQuery = new GestoreQuery();
		JsonArray risultato = gestoreQuery.esegui(questoJson, null, jsonUtili, mappaWhere);
		return risultato.toString();
		
	}

}
