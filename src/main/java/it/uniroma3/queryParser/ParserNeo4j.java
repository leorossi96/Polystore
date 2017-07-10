package it.uniroma3.queryParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import net.sf.jsqlparser.JSQLParserException;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ParserNeo4j implements QueryParser{
	private List<String> listaProiezioni;
	private List<String> tableList;
	private List<List<String>> matriceWhere;


	@Override
	public void spezza(String cypherQuery) throws JSQLParserException, FileNotFoundException{	
		//creo la lista from
		this.tableList = new LinkedList<>(); 
		ClassLoader classLoader = getClass().getClassLoader();
		File fileJSON = new File(classLoader.getResource("fileJSON.txt").getFile());
		Scanner scanner = new Scanner(fileJSON);
		while (scanner.hasNextLine()) {			
			String line = scanner.nextLine();
			JsonParser parser = new JsonParser();
			JsonObject myJson = parser.parse(line).getAsJsonObject();
			String table = myJson.get("table").getAsString();
			if (cypherQuery.toLowerCase().contains(table.toLowerCase()))
				this.tableList.add(table);
		}

		scanner.close();
		System.out.println("Parser Neo4j lista tabelle = "+tableList.toString());

		//creo la listaWhere
		this.matriceWhere = new LinkedList<>();
		String[] parti = cypherQuery.split(" WHERE ");
		String[] parti2 = parti[1].split(" RETURN ");

		String oggettoStringaWhere = parti2[0];
		String[] oggettiStatement = oggettoStringaWhere.split(" AND ");
		for (int i=0; i<oggettiStatement.length; i++){
			String[] oggettiStatementSeparati = oggettiStatement[i].split("=");
			List<String> rigaMatrice = new LinkedList<>();
			rigaMatrice.add(oggettiStatementSeparati[0].replaceAll("\\s+","")); //st = st.replaceAll("\\s+","")
			oggettiStatementSeparati[1] = oggettiStatementSeparati[1].replaceFirst("\\s+","");
			if (oggettiStatementSeparati[1].endsWith(" ")){
				oggettiStatementSeparati[1] = oggettiStatementSeparati[1].substring(0,oggettiStatementSeparati[1].length() - 1);
			}
			rigaMatrice.add(oggettiStatementSeparati[1]);
			this.matriceWhere.add(rigaMatrice);	
			System.out.println("\n\nNEO4J MATRICE WHERE =\n"+matriceWhere.toString()+"\n\n");
		} 	

		//creo la listaSelect
		this.listaProiezioni = new LinkedList<>();
		if (cypherQuery.contains(" Return ") || cypherQuery.contains(" RETURN ")){
			String oggettoStringaReturn = parti2[1];
			String[] partiReturn = oggettoStringaReturn.split("\\,");
			for (int i=0; i<partiReturn.length; i++){
				this.listaProiezioni.add(partiReturn[i]);
			}
		}
		
	}
	@Override
	public List<String> getListaProiezioni() {
		return listaProiezioni;
	}
	@Override
	public void setListaProiezioni(List<String> listaProiezioni) {
		this.listaProiezioni = listaProiezioni;
	}
	@Override
	public List<String> getListaTabelle() {
		return tableList;
	}
	@Override
	public void setListaTabelle(List<String> listaFrom) {
		this.tableList = listaFrom;
	}
	
	@Override
	public List<List<String>> getMatriceWhere() {
		return matriceWhere;
	}

	@Override
	public void setMatriceWhere(List<List<String>> matriceWhere) {
		this.matriceWhere = matriceWhere;
	}

}

