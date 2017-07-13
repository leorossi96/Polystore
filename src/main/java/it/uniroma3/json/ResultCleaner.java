package it.uniroma3.json;

import java.io.StringReader;
import java.util.Iterator;
import java.util.regex.Pattern;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

public class ResultCleaner {

	public static JsonArray fromMongo(JsonArray ris) {
		/*
		 * I risultati di una query mongo sono nella stessa forma dei risultati di una
		 * query SQL. Per questo motivo viene utilizzato lo stesso ResultCleaner.
		 */
		return fromSQL(ris);
	}

	public static JsonArray fromNeo4j(JsonArray ris, boolean joinRisultati, boolean isFiglio) {
		StringBuilder sb = new StringBuilder();
		Iterator<JsonElement> iterator = ris.iterator();
		while (iterator.hasNext()) {
			sb.append(iterator.next().toString());
		}
		String risultati = sb.toString();
		String r;
		if(joinRisultati && !isFiglio) // per risolvere il problema dei risultati nel caso in cui si abbia un join interno e quindi i risultati sono nella forma [{"payment":{"amount":10,"payment_id":1,"staff_id":1,"customer_id":1,"rental_id":1},"rental":{"inventory_id":1,"staff_id":1,"customer_id":1,"rental_id":1}},{"payment":{"amount":10,"payment_id":2,"staff_id":2,"customer_id":1,"rental_id":2},"rental":{"inventory_id":2,"staff_id":2,"customer_id":1,"rental_id":2}},{"payment":{"amount":20,"payment_id":3,"staff_id":2,"customer_id":2,"rental_id":3},"rental":{"inventory_id":7,"staff_id":2,"customer_id":2,"rental_id":3}}]
			r = risultati.replaceAll("\\},\"([^:\\{].*?)\":\\{", ",")
			.replaceAll("\\{\"([^:\\{].*?)\":\\{\"", "\\{\"")
			.replaceAll(Pattern.quote("}{"), "},{")
			.replaceAll(Pattern.quote("}}"),"}");
		else{
			r = risultati.replaceAll("\\{\"([^:\\{].*?)\":\\{","\\{")
					.replaceAll(Pattern.quote("}}"),"}")
					.replaceAll(Pattern.quote("}{"), "},{");
		}
		String r2 = "["+r+"]";
		return createJsonArray(r2);
	}

	public static JsonArray fromSQL(JsonArray ris) {
		StringBuilder sb = new StringBuilder();
		Iterator<JsonElement> iterator = ris.iterator();
		while (iterator.hasNext()) {
			sb.append(iterator.next().toString());
		}
		String risultati = sb.toString();
		String r = risultati.replaceAll(Pattern.quote("\\\""), "\"")
				.replaceAll(Pattern.quote("{\"value\":\"{"), "{")
				.replaceAll(Pattern.quote("}\""), "")
				.replaceAll(":([^\"].*?),", ":\"$1\",")
				.replaceAll(Pattern.quote("}{"), "},{")
				.replaceAll("([0-9]).0", "$1");
		String r2 = "["+r+"]";
		return createJsonArray(r2);
	}

	private static JsonArray createJsonArray(String s) {
		JsonReader j = new JsonReader(new StringReader(s));
		j.setLenient(true);
		return new JsonParser().parse(j).getAsJsonArray();
	}

}
