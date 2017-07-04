package it.uniroma3.JsonUtils;

import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.JSQLParserException;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class FabbricatoreMappaStatement {

	//	public void creaMappaWhere(List<List<String>> matriceWhere, Map<String, JsonObject> jsonCheMiServono ) throws UnknownHostException, FileNotFoundException, JSQLParserException {
	//		
	//	    
	//	    JsonObject myjson = new JsonObject();
	//		Set<String> tabelle = jsonCheMiServono.keySet();
	//		
	//		
	//		
	//		for (String s : tabelle){
	//			List<List<String>> matriceWherePreciso = new LinkedList<>();
	//			myjson = jsonCheMiServono.get(s);
	//			JsonArray attributi = myjson.getAsJsonArray("members");
	//			for(int i=0; i<attributi.size(); i++){
	//				String attributo = attributi.get(i).getAsString();	
	//				for(List<String> rigaMatriceWhere : matriceWhere){
	//					if (attributo.equals(rigaMatriceWhere.get(0))){
	//						matriceWherePreciso.add(rigaMatriceWhere);
	//					}
	//				}
	//			}
	//			mappaWhere.put(s, matriceWherePreciso);
	//		}
	//	}

	public Map<String, List<List<String>>> creaMappaWhere(List<List<String>> matriceWhere, Map<String, JsonObject> jsonCheMiServono ) throws UnknownHostException, FileNotFoundException, JSQLParserException {

		Map<String, List<List<String>>> mappaWhere = new HashMap<>();

		Set<String> tabelle = jsonCheMiServono.keySet();
		
		for(String tabella:tabelle){
			mappaWhere.put(tabella, new LinkedList<List<String>>());
		}


		for(List<String> rigaMatrice: matriceWhere){
//			String parola = rigaMatrice.get(0).split("\\.")[1].split("_id")[0];
			String tabellaPrimaCondizione = rigaMatrice.get(0).split("\\.")[0];
			String tabellaSecondaCondizione = rigaMatrice.get(1).split("\\.")[0];
			if(tabellaSecondaCondizione != null && tabelle.contains(tabellaSecondaCondizione)){
//				if(tabellaPrimaCondizione.equals(parola)){ //aggiungo la riga normalmente
					mappaWhere.get(tabellaPrimaCondizione).add(rigaMatrice);
//				}
//				else{//aggiungo la riga invertita alla tabella seconda condizione
//					List<String> rigaMatriceInvertita = new LinkedList<>();
//					rigaMatriceInvertita.add(rigaMatrice.get(1));
//					rigaMatriceInvertita.add(rigaMatrice.get(0));
//					mappaWhere = this.addMappaWhere(mappaWhere, tabellaSecondaCondizione, rigaMatriceInvertita);
//				}
			}
			else{ // aggiungilo a tabella nella mappaWhere
				mappaWhere.get(tabellaPrimaCondizione).add(rigaMatrice);
			}
		}
		return mappaWhere;
	}

	public Map<String, List<String>> creaMappaSelect(List<String> listaProiezioni) {
		Map<String, List<String>> mappaSelect = new HashMap<>();
		for(String proiezione : listaProiezioni){
			String tabellaProiezione = proiezione.split("\\.")[0];
			if(mappaSelect.containsKey(tabellaProiezione))
				mappaSelect.get(tabellaProiezione).add(proiezione);
			else{
				List<String> proiezioniTabella = new LinkedList<>();
				proiezioniTabella.add(proiezione);
				mappaSelect.put(tabellaProiezione, proiezioniTabella);
			}
		}
		return mappaSelect;
	}
}
