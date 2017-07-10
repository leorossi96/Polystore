package it.uniroma3.queryParser;

import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import it.uniroma3.grafiPriotita.CaricatoreJson;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ParserMongo implements QueryParser {

	private List<String> tabelle;
	private List<String> listaProiezioni;
	private List<List<String>> matriceWhere;

	public ParserMongo() {
		this.tabelle = new LinkedList<>();
		this.matriceWhere = new LinkedList<>();
		this.listaProiezioni = new LinkedList<>();
	}

	@Override
	public List<String> getListaTabelle() {
		return tabelle;
	}

	@Override
	public void setListaTabelle(List<String> tabelle) {
		this.tabelle = tabelle;
	}

	@Override
	public List<String> getListaProiezioni() {
		return listaProiezioni;
	}

	@Override
	public void setListaProiezioni(List<String> colonneProiezioni) {
		this.listaProiezioni = colonneProiezioni;
	}

	@Override
	public List<List<String>> getMatriceWhere() {
		List<List<String>> nuovaMatriceWhere = new LinkedList<>();
		for (List<String> condizione : this.matriceWhere){ //per sostituire le "" con '', altrimenti non funziona;
			String elemento = condizione.get(1);
			char c = elemento.charAt(0);
			if(c=='"'){
				elemento = elemento.substring(1, elemento.length()-1);
				elemento = "'"+elemento+"'";
			}
			condizione.set(1, elemento);
			nuovaMatriceWhere.add(condizione);
		}
		matriceWhere = nuovaMatriceWhere;
		return matriceWhere;
	}

	@Override
	public void setMatriceWhere(List<List<String>> matriceWhere) {
		this.matriceWhere = matriceWhere;
	}

	@Override
	public void spezza (String queryMongo) throws UnknownHostException, FileNotFoundException{
		String tabella = this.dammiTabella(queryMongo);
		this.tabelle.add(tabella);	
		String statement = this.dammiQuery(queryMongo);
		//controllare se statment isEmpty
		JsonParser parser = new JsonParser();
		JsonArray myJsonArray = parser.parse(statement).getAsJsonArray();
		JsonObject myJson = myJsonArray.get(0).getAsJsonObject();
		if(myJsonArray.size()>1){
			JsonObject colonneDaSelezionare = myJsonArray.get(1).getAsJsonObject();
			aggiornaColonneProiezioni(colonneDaSelezionare);
		}
		aggiornamentoTabelleECondizioni(this.matriceWhere, this.tabelle.get(0), myJson);	
	}

	private void aggiornaColonneProiezioni(JsonObject colonneDaSelezionare) {
		Set<Entry<String, JsonElement>> entrySet = colonneDaSelezionare.entrySet();
		for(Map.Entry<String,JsonElement> entry : entrySet){
			this.listaProiezioni.add(entry.getKey());
		}

	}

	private void aggiornamentoTabelleECondizioni(List<List<String>> matriceWhere, String tabella, JsonObject myJson) throws FileNotFoundException {

		//trasformo il jsonObject in una mappa
		Map<String, Object> attributes = new HashMap<String, Object>();
		Set<Entry<String, JsonElement>> entrySet = myJson.entrySet();
		for(Map.Entry<String,JsonElement> entry : entrySet){
			attributes.put(entry.getKey(), myJson.get(entry.getKey()));
		}

		for(Map.Entry<String,Object> att : attributes.entrySet()){
			List<String> rigaMatrice = new LinkedList<>();
			rigaMatrice.add(att.getKey());
			rigaMatrice.add(att.getValue().toString());
			this.matriceWhere.add(rigaMatrice);	
		}
		List<String> tabelleAppoggio = new LinkedList<>();
		tabelleAppoggio.add(tabella);
		JsonObject nuovoJson = null;
		CaricatoreJson caricatoreJson = new CaricatoreJson();
		Map<String, JsonObject> jsonUtili = caricatoreJson.caricaJSON(tabelleAppoggio);//carico da file i json utili in base alle tabelle
		JsonObject tabellaInformazioni = jsonUtili.get(tabella);
		JsonArray tabelleCheConosce = tabellaInformazioni.get("knows").getAsJsonArray();
		for (int i=0; i<tabelleCheConosce.size();i++){
			String tabellaCheConosce = tabelleCheConosce.get(i).getAsJsonObject().get("table").getAsString();
			String foreignKey = tabelleCheConosce.get(i).getAsJsonObject().get("foreignkey").getAsString();
			List<List<String>> matriceWhereCopia = new LinkedList<>(matriceWhere);
			for(List<String> condizione : matriceWhereCopia){
				List<String> nuovaCondizione = new LinkedList<>();
				if((condizione.get(0).equals(tabellaCheConosce)) && myJson.get(tabellaCheConosce)!=null){ //se ho dei join, saranno espressi in mongo come jsonObject
					rimuoviElemento(tabellaCheConosce, this.matriceWhere);
					nuovoJson = myJson.get(tabellaCheConosce).getAsJsonObject();
					this.tabelle.add(tabellaCheConosce);
					jsonUtili = caricatoreJson.caricaJSON(this.tabelle);
					String pkTabellaCheConosce = jsonUtili.get(tabellaCheConosce).get("primarykey").getAsString();
					nuovaCondizione.add(foreignKey);
					nuovaCondizione.add(pkTabellaCheConosce);
					this.matriceWhere.add(nuovaCondizione);
					aggiornamentoTabelleECondizioni(this.matriceWhere, tabellaCheConosce, nuovoJson);
				}
			}
		}


	}

	private void rimuoviElemento(String tabellaCheConosce, List<List<String>> matriceWhere2) {
		int i=0;
		int iteratore=0;
		for (List<String> condizione: matriceWhere2){
			if(condizione.get(0).equals(tabellaCheConosce))
				iteratore = i;
			i++;
		}
		this.matriceWhere.remove(iteratore);

	}

	private String dammiTabella(String queryMongo){
		String[] parti = queryMongo.split("\\.");
		String tabella = parti[1];
		return tabella;
	}

	private String dammiQuery(String queryMongo){
		String[] parti = queryMongo.split("\\.",3);
		String parte3 = parti[2];
		parti =  parte3.split("\\(");
		String queryNONbuona = parti[1];
		String query = queryNONbuona.replaceAll("\\)", "]");
		query = "[" + query;
		return query;
	}
}