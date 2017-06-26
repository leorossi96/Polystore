package it.uniroma3.costruttoreQuery;

import java.io.StringReader;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.stream.JsonReader;

import it.uniroma3.JsonUtils.Convertitore;
import it.uniroma3.persistence.neo4j.GraphDao;

public class CostruttoreQueryNeo4j implements CostruttoreQuery {

	//	@Override
	//	public void eseguiQuery(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto,
	//			List<String> nodo, Map<String, List<List<String>>> mappaWhere, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita)
	//					throws Exception {
	//		StringBuilder queryRiscritta = new StringBuilder();
	//		StringBuilder queryCreateNodi = new StringBuilder();
	//		StringBuilder queryCreateRelazioni = new StringBuilder();
	//		StringBuilder queryDelete = new StringBuilder();
	//
	//
	//		List<String>nodiCandidatiPerJoinConRisultati = new LinkedList<>();
	//
	//		//Costruisco la query
	//		String tabellaPartenza = nodo.get(0);
	//		queryRiscritta.append("MATCH ("+tabellaPartenza+" : "+tabellaPartenza+")\n");
	//		for(int i=0; i<nodo.size(); i++){
	//			List<List<String>> condizioniTabella = mappaWhere.get(nodo.get(i));
	//			if (condizioniTabella.size() == 0)	{
	//				queryRiscritta.append("RETURN "+tabellaPartenza);
	//			}
	//
	//			for(int j=0; j<condizioniTabella.size(); j++){
	//				List<String> condizione = condizioniTabella.get(j);
	//				String primaParolaParametro = condizione.get(1).split("\\.")[0];
	//				if(nodo.contains(primaParolaParametro)) //non è una condizione di join
	//					queryRiscritta.append("MATCH ("+nodo.get(i)+" : "+nodo.get(i)+")--("+primaParolaParametro+" : "+primaParolaParametro+")\n");
	//				else if(!mappaWhere.keySet().contains(primaParolaParametro))
	//					queryRiscritta.append("WHERE "+condizione.get(0)+" = "+condizione.get(1)+"\n");
	//				else
	//					nodiCandidatiPerJoinConRisultati.add(nodo.get(i)+","+condizione.get(1));  //[(customer,address.address_id)]
	//
	//
	//				queryRiscritta.append("RETURN { ");
	//				for(int n=0; n<nodo.size()-1; j++){
	//					queryRiscritta.append(nodo.get(n)+" : "+nodo.get(n)+", ");
	//				}
	//				queryRiscritta.append(nodo.get(nodo.size()-1)+" : "+nodo.get(nodo.size()-1)+" }");
	//
	//				//		match(rental:rental)
	//				//		match (rental:rental)--(payment:payment)
	//				//		where payment.amount>5
	//				//		match (payment:payment)--(store:Store)
	//				//		where store.qualcosa=1
	//				//		return {rental : rental , payment : payment, store : store}
	//				//VEDI FORMATO RISULTATO PER CONVERTIRLO IN JSON
	//
	//
	//
	//				for(DefaultWeightedEdge arco : grafoPrioritaCompatto.outgoingEdgesOf(nodo)){
	//					JsonArray risFiglio = mappaRisultati.get(grafoPrioritaCompatto.getEdgeTarget(arco));
	//					List<String> nodoFiglio = grafoPrioritaCompatto.getEdgeTarget(arco);
	//					int y = 0;
	//					for(JsonElement e : risFiglio){
	//						y++;
	//						queryCreateNodi.append("CREATE (ris"+y+" : risultato "+e.getAsString()+")");
	//						//1
	//						eseguiQueryDirettamente(queryCreateNodi);
	//						//								create (node1:type {name:'example1', type:'example2'})
	//					}
	//
	//					for(int z=0; i<nodiCandidatiPerJoinConRisultati.size(); z++){
	//						String nodoCandidato = nodiCandidatiPerJoinConRisultati.get(z).split(",")[0]; //customer
	//						String foreignKey = nodiCandidatiPerJoinConRisultati.get(z).split(",")[1]; //address.address_id
	//						if(nodoFiglio.contains(foreignKey.split("\\.")[0])){
	//							String nodoBQuery = nodoCandidato;
	//							String foreignKeyPerQuery = foreignKey.split("\\.")[1];//address_id
	//							queryCreateRelazioni.append("MATCH (a : risultato), (b : "+nodoBQuery+")\nWHERE a."+foreignKeyPerQuery+" = b."+foreignKeyPerQuery+"\nCREATE (b)-[:TEMP REL]->(b)");
	//							//2
	//							eseguiQueryDirettamente(queryCreateRelazioni);
	//
	//							//							MATCH (a:Person),(b:Country)
	//							//							WHERE HAS (a.id) AND HAS (b.id) AND a.id=b.id
	//							//							CREATE (a)-[:LIVES]->(b);
	//						}
	//					}
	//				}
	//			}
	//			JsonArray risultati = eseguiQueryDirettamente(queryRiscritta);
	//			JsonArray risutatiFormaCorretta = this.pulisciRisultati(risultati);
	//
	//			System.out.println("QUERY NEO4J ="+queryRiscritta.toString());
	//
	//			//3
	//			mappaRisultati.put(nodo, risutatiFormaCorretta);
	//			//4
	//			//				queryDelete.append("MATCH (n : risultato)\nDETACH DELETE n");//cancella i nodi risultato e i relativi collegamenti
	//			//				System.out.println("QUERY NEO4J ="+queryRiscritta.toString());
	//			//				eseguiQueryDirettamente(queryDelete);
	//			//				
	//
	//		}
	//	}

	@Override
	public void eseguiQuery(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto,
			List<String> nodo, Map<String, List<List<String>>> mappaWhere, Map<List<String>, JsonArray> mappaRisultati, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita)
					throws Exception {
		//creo una mappa che ha come chiave il nome della fk dei figli e come valore la lista delle fk da unsare nella funzione IN 
		Map<String , List<String>> mappaArrayFkFigli = new HashMap<>(); //address.address_id ->["1","2"], customer.customer_id ->["4","9"]
		for(DefaultWeightedEdge arco : grafoPrioritaCompatto.outgoingEdgesOf(nodo)){
			String tabellaFiglio = grafoPrioritaCompatto.getEdgeTarget(arco).get(0);
			JsonArray risFiglio = mappaRisultati.get(grafoPrioritaCompatto.getEdgeTarget(arco));
			for(JsonElement e: risFiglio){
				JsonObject jo = e.getAsJsonObject();
				if(!mappaArrayFkFigli.containsKey(tabellaFiglio+"."+tabellaFiglio+"_id")){
					mappaArrayFkFigli.put(tabellaFiglio+"."+tabellaFiglio+"_id", new LinkedList<String>());
					mappaArrayFkFigli.get(tabellaFiglio+"."+tabellaFiglio+"_id").add(jo.get(tabellaFiglio+"_id").getAsString());
				}
				else
					mappaArrayFkFigli.get(tabellaFiglio+"."+tabellaFiglio+"_id").add(jo.get(tabellaFiglio+"_id").getAsString());
			}
		}
		System.out.println("MAPPA ARRAY FK FIGLI = "+mappaArrayFkFigli.toString());
		StringBuilder queryRiscritta = new StringBuilder();
		boolean isFiglio = true;
		String campoReturn = this.getForeingKeyNodo(grafoPriorita,nodo.get(0),mappaWhere);
		if(campoReturn == null){
			isFiglio = false;
		}
		String tabellaPartenza = nodo.get(0);
		queryRiscritta.append("MATCH ("+tabellaPartenza+" : "+tabellaPartenza+")\n");
		for(int i=0; i<nodo.size(); i++){
			List<List<String>> condizioniTabella = mappaWhere.get(nodo.get(i));
			//			if (condizioniTabella.size() == 0)	{
			//				queryRiscritta.append("RETURN "+tabellaPartenza);
			//			}				
			for(int j=0; j<condizioniTabella.size(); j++){
				List<String> condizione = condizioniTabella.get(j);
				String primaParolaParametro = condizione.get(1).split("\\.")[0];
				if(nodo.contains(primaParolaParametro)) //non è una condizione di join
					queryRiscritta.append("MATCH ("+nodo.get(i)+" : "+nodo.get(i)+")--("+primaParolaParametro+" : "+primaParolaParametro+")\n");
				else if(!mappaWhere.keySet().contains(primaParolaParametro))
					queryRiscritta.append("WHERE "+condizione.get(0)+" = "+condizione.get(1)+"\n");
				else {
					queryRiscritta.append("WHERE "+condizione.get(0)+" IN "+mappaArrayFkFigli.get(condizione.get(1).toString())+"\n");
					//						nodiCandidatiPerJoinConRisultati.add(nodo.get(i)+","+condizione.get(1));  //[(customer,address.address_id)]
				}
			}
		}
		if(isFiglio){
			queryRiscritta.append("RETURN "+campoReturn); // da vedere come esce output di questo risultato
		}
		else{
			queryRiscritta.append("RETURN { ");
			for(int n=0; n<nodo.size()-1; n++){
				queryRiscritta.append(nodo.get(n)+" : "+nodo.get(n)+", ");
			}
			queryRiscritta.append(nodo.get(nodo.size()-1)+" : "+nodo.get(nodo.size()-1)+" }\n");
		}

		//		match(rental:rental)
		//		match (rental:rental)--(payment:payment)
		//		where payment.amount>5
		//		match (payment:payment)--(store:Store)
		//		where store.qualcosa=1
		//		return {rental : rental , payment : payment, store : store}
		//VEDI FORMATO RISULTATO PER CONVERTIRLO IN JSON


		System.out.println("QUERY NEO4J =\n"+ queryRiscritta.toString());


		JsonArray risultati = eseguiQueryDirettamente(queryRiscritta);
		JsonArray risutatiFormaCorretta = this.pulisciRisultati(risultati);
		mappaRisultati.put(nodo, risutatiFormaCorretta);		

	}



	/**
	 * 
	 * @return La chiave esterna sulla quale il nodo padre effettua il join. Null altrimenti.
	 */
	private String getForeingKeyNodo(SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita, String nodo, Map<String, List<List<String>>> mappaWhere) {
		String padre = null;
		for(DefaultWeightedEdge arco : grafoPriorita.incomingEdgesOf(nodo)){
			padre = grafoPriorita.getEdgeSource(arco);
		}
		if(padre != null){
			for(List<String> condizioni : mappaWhere.get(padre)){
				StringTokenizer st = new StringTokenizer(condizioni.get(1), ".");
				if(st.nextToken().equals(nodo))
					return condizioni.get(1);
			}
		}
		return null;
	}

	private JsonArray pulisciRisultati(JsonArray ris) {
		StringBuilder sb = new StringBuilder();
		Iterator<JsonElement> iterator = ris.iterator();
		while (iterator.hasNext()) {
			sb.append(iterator.next().toString());
		}
		String risultati = sb.toString();
		System.out.println("RISULTATI ="+risultati+"\n");
		String r = risultati.replaceAll("\\{\"([^:\\{].*?)\":\\{","\\{").replaceAll(Pattern.quote("}}"),"}").replaceAll(Pattern.quote("}{"), "},{");
		String r2 = "["+r+"]";
		System.out.println("RISULTATI CORRETTI ="+r2); 
		StringReader s = new StringReader(r2);
		JsonReader j = new JsonReader(s);
		j.setLenient(true);
		return new JsonParser().parse(j).getAsJsonArray();
	}

	private JsonArray eseguiQueryDirettamente(StringBuilder queryRiscritta) throws Exception{
		GraphDao dao = new GraphDao();
		ResultSet risultatoResult = dao.interroga(queryRiscritta.toString());
		JsonArray risultati = Convertitore.convertCypherToJson(risultatoResult);
		return risultati;
	}
}
