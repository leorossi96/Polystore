package it.uniroma3.controller;



import javax.servlet.http.HttpServletRequest;


import com.google.gson.Gson;
import com.google.gson.JsonArray;

import java.lang.reflect.Type;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.gson.reflect.TypeToken;

import it.uniroma3.polystoreMain.Polystore;


/**
 * Analizza il tipo di query. A seconda del tipo di query, invoca la facade giusta
 * @author micheletedesco1
 *
 */

public class Action {
	private Set<String> attributi;
	private List<List<Object>> matriceRisultati;

	@SuppressWarnings("finally")
	public String execute(HttpServletRequest request) throws Exception {
		try{
		String query = request.getParameter("query");
		JsonArray risultato = new JsonArray();
		if(!query.isEmpty()){ 
			new Polystore().run(query);
			}		
		else {
			request.setAttribute("queryError", "campo obbligatorio");
			return "/index.jsp";
		}
		if(risultato.size()==0){
			request.setAttribute("queryError", "LA QUERY INSERITA NON HA PRODOTTO RISULTATO. ELEMENTO NON PRESENTE");
		}else{
			convertForPrinting(risultato);
			request.setAttribute("attributi", this.attributi);
			request.setAttribute("matriceRisultati", this.matriceRisultati);
			request.setAttribute("query", query);
			//return "/risultati.jsp";
		}
		}catch (Exception e) {
			request.setAttribute("queryError", "QUERY NON INSERITA CORRETTAMENTE");
			e.printStackTrace();
		} finally{
			return "/index.jsp";
		}
	}

	/**
	 * Converte il risultato (JsonArray) in un formato adatto per la stampa (lista di liste di oggetti)
	 * @param risultato
	 */
	private void convertForPrinting(JsonArray risultato) { //solo per poi stampare e far vedere i risultati
		this.matriceRisultati = new LinkedList<>();
		Gson gson = new Gson();
		Type type = new TypeToken<Map<String, Object>>(){}.getType();
		Map<String, Object> riga = gson.fromJson(risultato.get(0), type);
		this.attributi = riga.keySet();
		for (int i=0; i<risultato.size();i++){
			Map<String, Object> rigaInFormatoMappa = gson.fromJson(risultato.get(i), type);
			List<Object> rigaRisultato = new LinkedList<>();
			for(String s:attributi){
				rigaRisultato.add(rigaInFormatoMappa.get(s));	
			}	
			this.matriceRisultati.add(rigaRisultato);
		}
		
	}
}
