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

/**
 * Questa classe ha il compito di creare da una collezione di condizioni "sparse" una mappa di condizioni associate
 * a quella particolare tabella (key = tabelle da interrogare, value = condizioni associate
 * a quelle tabelle). ES: customer = [[customer.nome, mario],[customer.id_store,1]]
 * @author micheletedesco1
 *
 */
public class FabbricatoreMappaCondizioni {
	private Map<String, List<List<String>>> mappaWhere;
	
	
	public FabbricatoreMappaCondizioni() {
		this.mappaWhere = new HashMap<>();
	}

	/**
	 * 
	 * @param matriceWhere una collezione di condizioni ES:[[customer.nome, mario],[customer.id_store,1]]
	 * @param jsonCheMiServono una collezione di JsonObject contenenti informazioni sulle tabelle da interrogare
	 */
	public void creaMappaCondizioni(List<List<String>> matriceWhere, Map<String, JsonObject> jsonCheMiServono ) throws UnknownHostException, FileNotFoundException, JSQLParserException {
		
	    
	    JsonObject myjson = new JsonObject();
		Set<String> tabelle = jsonCheMiServono.keySet();
		
		
		
		for (String s : tabelle){
			List<List<String>> matriceWherePreciso = new LinkedList<>();
			myjson = jsonCheMiServono.get(s);
			JsonArray attributi = myjson.getAsJsonArray("members");
			for(int i=0; i<attributi.size(); i++){
				String attributo = attributi.get(i).getAsString();	
				for(List<String> rigaMatriceWhere : matriceWhere){
					if (attributo.equals(rigaMatriceWhere.get(0)))
						matriceWherePreciso.add(rigaMatriceWhere);
				}
			}
			mappaWhere.put(s, matriceWherePreciso);
		}
	}


	public Map<String, List<List<String>>> getMappaWhere() {
		return mappaWhere;
	}


	public void setMappaWhere(Map<String, List<List<String>>> mappaWhere) {
		this.mappaWhere = mappaWhere;
	}
	
	

}
