package it.uniroma3.queryParser;

import java.util.List;

public class ParserMongo implements QueryParser {

	@Override
	public void spezza(String querySQL) throws Exception {
		
	}

	@Override
	public void setListaTabelle(List<String> listaFrom) {
		
	}

	@Override
	public List<String> getListaTabelle() {
		return null;
	}

	@Override
	public List<List<String>> getMatriceWhere() {
		return null;
	}

	@Override
	public void setMatriceWhere(List<List<String>> matriceWhere) {
	}

	@Override
	public List<String> getListaProiezioni() {
		return null;
	}

	@Override
	public void setListaProiezioni(List<String> listaProiezioni) {	
	}

}
