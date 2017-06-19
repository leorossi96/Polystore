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
	private Map<String, List<List<String>>> mappaWhere;
	
	
	public FabbricatoreMappaStatement() {
		this.mappaWhere = new HashMap<>();
	}


	public void creaMappaWhere(List<List<String>> matriceWhere, Map<String, JsonObject> jsonCheMiServono ) throws UnknownHostException, FileNotFoundException, JSQLParserException {
		
	    
	    JsonObject myjson = new JsonObject();
		Set<String> tabelle = jsonCheMiServono.keySet();
		
		
		
		for (String s : tabelle){
			List<List<String>> matriceWherePreciso = new LinkedList<>();
			myjson = jsonCheMiServono.get(s);
			JsonArray attributi = myjson.getAsJsonArray("members");
			for(int i=0; i<attributi.size(); i++){
				String attributo = attributi.get(i).getAsString();	
				for(List<String> rigaMatriceWhere : matriceWhere){
					if (attributo.equals(rigaMatriceWhere.get(0))){
						matriceWherePreciso.add(rigaMatriceWhere);
					}
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
