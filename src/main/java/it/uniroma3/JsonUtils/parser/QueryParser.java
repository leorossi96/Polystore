package it.uniroma3.JsonUtils.parser;

import java.util.List;

public interface QueryParser {
	
	public void spezza(String querySQL) throws Exception;
	public void setTableList(List<String> listaFrom);
	public List<String> getTableList();
	public List<List<String>> getMatriceWhere();
	public void setMatriceWhere(List<List<String>> matriceWhere);
	

}
