package it.uniroma3.json;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.google.gson.JsonArray;

public class JsonWriter {

	/**
	 * Salva temporaneamente il JsonArray nella cartella temporanea di default.
	 * Il file viene cancellato quando il programma termina.
	 * @param jsonArray Il jsonArray da persistere.
	 * @return L'indirizzo assoluto del file.
	 * @throws IOException
	 */
	public String writeArrayTemporary(JsonArray jsonArray) throws IOException {
		File file = File.createTempFile("polystore", ".json");
		file.deleteOnExit();
		FileWriter writer = new FileWriter(file);
		writer.write(jsonArray.toString());
		writer.close();
		return file.getAbsolutePath();
	}


	/**
	 * Salva su un file il jsonArray.
	 * Se il file non esiste viene creato, altrimenti viene sovrasctitto il vecchio.
	 * @param jsonArray Il jsonArray da persistere.
	 * @param pathName Il path del file su cui scrivere.
	 * @throws IOException 
	 */
	public void writeArray(JsonArray jsonArray, String pathName) throws IOException {
		File file = new File(pathName);
		if(!file.exists())
			file.createNewFile();
		FileWriter writer = new FileWriter(file);
		writer.write(jsonArray.toString());
		writer.close();
	}
}
