package it.uniroma3.grafiPriotita;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class CaricatoreJson {

	/**
	 * Carica da file i json utili in base alle tabelle
	 * @param listaFrom Tabelle di partenza
	 * 
	 * @throws FileNotFoundException
	 */
	public Map<String, JsonObject> caricaJSON(List<String> listaFrom) throws FileNotFoundException{
		Map<String, JsonObject> jsonUtili = new HashMap<>();
		ClassLoader classLoader = getClass().getClassLoader();
		File fileJSON = new File(classLoader.getResource("fileJSON.txt").getFile());
		Scanner scanner = new Scanner(fileJSON);
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			for(String tabella : listaFrom){			
				JsonParser parser = new JsonParser();
				JsonObject myJson = parser.parse(line).getAsJsonObject();
				String table = myJson.get("table").getAsString();
				if(table.equals(tabella)){
					jsonUtili.put(tabella, myJson);
				}
			}
		}
		scanner.close();
		return jsonUtili;
	}

}
	