package it.uniroma3.queryParser;

import java.util.List;

public interface QueryParser {
	
	public void spezza(String querySQL) throws Exception;
	public void setListaTabelle(List<String> listaFrom);
	public List<String> getListaTabelle();
	public List<List<String>> getMatriceWhere();
	public void setMatriceWhere(List<List<String>> matriceWhere);
	public List<String> getListaProiezioni();
	public void setListaProiezioni(List<String> listaProiezioni);
	
}
