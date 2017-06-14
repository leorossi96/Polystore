package it.uniroma3.costruttoreQuery;

import java.util.List;
import java.util.Map;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import it.uniroma3.persistence.mongo.MongoDao;

public class CostruttoreQueryMongo implements CostruttoreQuery {

	//per aggiungere condizioni in una query aggregation si usa {$match:{"campo": "valore"}} e per la select si usa {$project:{"campo":1, "campo":1}}
	@Override
	public void eseguiQuery(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto,
			List<String> nodo, Map<String, List<List<String>>> mappaWhere, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph <String, DefaultWeightedEdge> grafoPriorita)
					throws Exception {
		//Le condizioni di elementi possono essere applicate solo riferendosi direttamente alla collection 
		//APPLICO L'AGGREGATION ALLA PRIMA COLLECTION DEL NODO
		List<List<String>> condizioniPerQuellaTabella = mappaWhere.get(nodo.get(0));
		BasicDBObject query = new BasicDBObject();
		for(int i=0; i<condizioniPerQuellaTabella.size(); i++){
			//estraggo la riga i-esima della matrice
			List<String> condizione = condizioniPerQuellaTabella.get(i);
			String parametro = condizione.get(0).replace(nodo.get(0)+".", "");
			String valore= condizione.get(1).replaceAll("'", "");
			//vedo se il valore è un numero o una stringa (mongoDB è poco intelligente sui tipi)
			Object value = null;
			if (Character.isDigit(valore.charAt(0)))
				value = Integer.parseInt(valore);
			else value = valore;
			query.put(parametro, value);
		}

//			LookupOperation lookupOperation = LookupOperation.newLookup()
//				    .from("places")
//				    .localField("address.location.place._id")
//				    .foreignField("_id")
//				    .as("address.location.place");
//
//				Aggregation agg = newAggregation(
//				    unwind("address"),
//				    unwind("address.location"),
//				    lookupOperation  
//				);
//
//				AggregationResults<OutputDocument> aggResults = mongoTemplate.aggregate(
//				    agg, PersonAddressDocument.class, OutputDocument.class
//				);
		}

		private JsonArray eseguiQueryDirettamente(DBObject queryRiscritta, String tabella) throws Exception { 
			MongoDao dao = new MongoDao();
			DBCursor cursor = dao.interroga(queryRiscritta,tabella);
			JsonArray documenti = new JsonArray();
			JsonParser parser = new JsonParser();
			while (cursor.hasNext()){
				BasicDBObject oggetto = (BasicDBObject) cursor.next();
				String documentoInFormatoStringa = oggetto.toString();
				JsonObject documentoInFormatoJson = parser.parse(documentoInFormatoStringa).getAsJsonObject();	
				documenti.add(documentoInFormatoJson);
			}
			return documenti;
		}

	}
