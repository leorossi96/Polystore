package it.uniroma3.json;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.google.gson.JsonArray;

public class JsonWriter {

	/**
	 * Salva su un file il jsonArray.
	 * Se il file non esiste viene creato, altrimenti viene sovrasctitto il vecchio.
	 * @param jsonArray Il jsonArray da persistere.
	 * @param pathName Il path del file su cui scrivere.
	 * @throws IOException 
	 */
	public void writeArray(JsonArray jsonArray, String pathName) throws IOException {
		File file = new File(pathName);
		try {
			if(!file.exists())
				file.createNewFile();
			FileWriter writer = new FileWriter(file);
			writer.write(jsonArray.toString());
			writer.close();
		} catch (IOException e) {
			System.out.println("Impossibile salvare i risultati nel file "+pathName);
			throw e;
		}
	}
}
