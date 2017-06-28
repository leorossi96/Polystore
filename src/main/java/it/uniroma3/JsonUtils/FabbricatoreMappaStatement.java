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
		



		for(List<String> rigaMatrice: matriceWhere){
			String parola = rigaMatrice.get(0).split("\\.")[1].split("_id")[0];
			String tabellaPrimaCondizione = rigaMatrice.get(0).split("\\.")[0];
			String tabellaSecondaCondizione = rigaMatrice.get(1).split("\\.")[0];
			if(tabellaSecondaCondizione != null && tabelle.contains(tabellaSecondaCondizione)){
				if(tabellaPrimaCondizione.equals(parola)){ //aggiungo la riga normalmente
					mappaWhere = this.addMappaWhere(mappaWhere, tabellaPrimaCondizione, rigaMatrice);
				}
				else{//aggiungo la riga invertita alla tabella seconda condizione
					List<String> rigaMatriceInvertita = new LinkedList<>();
					rigaMatriceInvertita.add(rigaMatrice.get(1));
					rigaMatriceInvertita.add(rigaMatrice.get(0));
					mappaWhere = this.addMappaWhere(mappaWhere, tabellaSecondaCondizione, rigaMatriceInvertita);
				}
			}
			else{ // aggiungilo a tabella nella mappaWhere
				mappaWhere = this.addMappaWhere(mappaWhere, tabellaPrimaCondizione, rigaMatrice);
			}
		}
		return mappaWhere;
	}
	
	private Map<String, List<List<String>>> addMappaWhere (Map<String, List<List<String>>> mappaWhere, String tabella, List<String> rigaDaInserire){
		if(mappaWhere.containsKey(tabella))
			mappaWhere.get(tabella).add(rigaDaInserire);
		else{
			List<List<String>> listaRigheMatrici = new LinkedList<>();
			listaRigheMatrici.add(rigaDaInserire);
			mappaWhere.put(tabella,listaRigheMatrici);
		}
		return mappaWhere;
	}
}
