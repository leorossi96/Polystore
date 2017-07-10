package it.uniroma3.grafiPriotita;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import com.google.gson.JsonObject;

public class FabbricatoreAlberoEsecuzione {
	
	public SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> getGrafoPriorita(List<String> tabelle, Map<String, List<List<String>>> mappaWhere) {
		SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
		for(int i=0; i<tabelle.size(); i++){
			String tabellaCorrente = tabelle.get(i);
			grafoPriorita.addVertex(tabellaCorrente);
			List<String> figli = getFigli(tabellaCorrente, mappaWhere, tabelle);
			for(int j=0; j<figli.size(); j++){
				String figlio = figli.get(j);
				grafoPriorita.addVertex(figlio);
				DefaultWeightedEdge e = grafoPriorita.addEdge(tabellaCorrente, figlio);
				grafoPriorita.setEdgeWeight(e, 0);
			}
		}
		return grafoPriorita;
	}
	
	public SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> getGrafoPrioritaCompatto(SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita, Map<String, JsonObject> jsonUtili, Map<String, List<String>> mappaDB){
		SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
		List<DefaultWeightedEdge> archiUtili = new LinkedList<>();
		for(String db : mappaDB.keySet()){
			List<String> nodi = mappaDB.get(db);
			Iterator<String> i = nodi.iterator();
			while(i.hasNext()){
				String nodoCorrente = i.next();
				Iterator<DefaultWeightedEdge> iArchi = grafoPriorita.incomingEdgesOf(nodoCorrente).iterator();
				System.out.println("nodo corrente :"+nodoCorrente);
				if(iArchi.hasNext()){
					DefaultWeightedEdge arcoEntrante = iArchi.next();
					String padre = grafoPriorita.getEdgeSource(arcoEntrante);
					if(!jsonUtili.get(padre).get("database").getAsString().equals(db)){
						List<String> nodoNuovoGrafo = new LinkedList<>();
						nodoNuovoGrafo.add(nodoCorrente);
						archiUtili.add(arcoEntrante);
						System.out.println("archi utili :"+archiUtili);
						addDiscendentiStessoDB(nodoNuovoGrafo, nodoCorrente, db, grafoPriorita, jsonUtili);
						grafoPrioritaCompatto.addVertex(nodoNuovoGrafo);
					}
				}
				else{ //caso in cui ci stiamo occupando della radice 
					List<String> nodoNuovoGrafo = new LinkedList<>();
					nodoNuovoGrafo.add(nodoCorrente);
					addDiscendentiStessoDB(nodoNuovoGrafo, nodoCorrente, db, grafoPriorita, jsonUtili);
					grafoPrioritaCompatto.addVertex(nodoNuovoGrafo);
				}
			}
			System.out.println("GRAFO PARZIALE:"+grafoPrioritaCompatto.toString());
		}
		//aggiungo gli archi
		for(DefaultWeightedEdge e : archiUtili){
			System.out.println("Arco Da Aggiungere:"+ e);
			for(List<String> nodoPadre : grafoPrioritaCompatto.vertexSet()){
				if(nodoPadre.contains(grafoPriorita.getEdgeSource(e))){
					System.out.println("nodoPadre = "+nodoPadre+"\n Contains = "+grafoPriorita.getEdgeSource(e));
					for(List<String> nodoFiglio : grafoPrioritaCompatto.vertexSet()){
						if(nodoFiglio.contains(grafoPriorita.getEdgeTarget(e))){
							System.out.println("nodoFiglio = "+nodoFiglio+"\n Contains = "+grafoPriorita.getEdgeTarget(e));
							grafoPrioritaCompatto.addEdge(nodoPadre, nodoFiglio);

						}
					}
				}	
			}
		}
		return grafoPrioritaCompatto;
	}

	private static void addDiscendentiStessoDB(List<String> nodoNuovoGrafo, String nodoCorrente, String db, SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> grafoPriorita, Map<String, JsonObject> jsonUtili) {
		Set<DefaultWeightedEdge> archi = grafoPriorita.outgoingEdgesOf(nodoCorrente);
		if(archi==null)
			return;
		else{
			Iterator<DefaultWeightedEdge> i = archi.iterator();
			while(i.hasNext()){
				DefaultWeightedEdge arco = i.next();
				String figlio = grafoPriorita.getEdgeTarget(arco);
				if(jsonUtili.get(figlio).get("database").getAsString().equals(db)){
					nodoNuovoGrafo.add(figlio);
					addDiscendentiStessoDB(nodoNuovoGrafo, figlio, db, grafoPriorita, jsonUtili);
				}
			}
		}
	}

	public SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> copiaGrafo (SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPriorita){
		SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> copia = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
		for(List<String> nodo : grafoPriorita.vertexSet()){
			List<String> nuovoNodo = new LinkedList<>(nodo);
			copia.addVertex(nuovoNodo);
		}
		for(DefaultWeightedEdge arco : grafoPriorita.edgeSet()){
			copia.addEdge(grafoPriorita.getEdgeSource(arco), grafoPriorita.getEdgeTarget(arco));
		}
		return copia;
	}
	
	private static List<String> getFigli(String tabella, Map<String, List<List<String>>> mappaWhere, List<String> tabelle){
		List<String> figli = new LinkedList<>();
		List<List<String>> condizioniTabella = mappaWhere.get(tabella);
		for(int i=0; i<condizioniTabella.size(); i++){
			List<String> condizione = condizioniTabella.get(i);
			if(tabelle.contains(condizione.get(1).split("\\.")[0]))
				figli.add(condizione.get(1).split("\\.")[0]);
		}
		return figli;
	}
	
	public List<String> getRadice(SimpleDirectedWeightedGraph<List<String>, DefaultWeightedEdge> grafoPrioritaCompatto) {
		Iterator<List<String>> i = grafoPrioritaCompatto.vertexSet().iterator();
		List<String> radice = null;
		while(i.hasNext()){
			List<String> nodo = i.next();
			if(grafoPrioritaCompatto.incomingEdgesOf(nodo).isEmpty()){
				radice = nodo;
			}
		}
		return radice;
	}

}
