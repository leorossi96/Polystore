package it.uniroma3.model.facade;



import it.uniroma3.JsonUtils.CaricatoreJSON;
import it.uniroma3.JsonUtils.FabbricatoreMappaCondizioni;
import it.uniroma3.JsonUtils.GestoreQuery;
import it.uniroma3.JsonUtils.parser.ParserSql;

import java.util.List;
import java.util.Map;









import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

/**
 * Data la query SQL iniziale, interroga tutte le tabelle e restituisce il risultato
 * @author micheletedesco1
 *
 */

/*
 * Questa classe funge da WorkflowManager data una query SQL iniziale, interrogando le varie tabelle in cascata seguendo i collegamenti (join).
 * In pratica, data la tabella con priorità più alta, da questa partiro e navighero tutti i collegamenti richiesti (si veda la classe GestoreQuery).
 * ES: SELECT customer.name FROM customer, store WHERE customer.store_id = store.id AND store.address_id = 1 
 * Parto dalla tabella customer------>store. Vedo che in customer è presente un join con store, quindi eseguo store : MATCH (store:store) WHERE store.address_id = 1 RETURN store.id
 * I risultati ottenuti andranno in customer: SELECT * FROM customer WHERE  customer.store_id = i risultati della query precedente.
 * @author micheletedesco1
 *
 */
public class FacadeSQL {
	
	public String gestisciQuery(String querySQL) throws Exception{
		ParserSql parser = new ParserSql();
		parser.spezza(querySQL);//spezzo la query
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
