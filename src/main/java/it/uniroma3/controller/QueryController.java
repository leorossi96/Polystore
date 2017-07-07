package it.uniroma3.controller;



import it.uniroma3.grafiPriotita.Polystore;


public class QueryController {

	private String query;
	

	public String executeQuery(){
		Polystore polystore = new Polystore();
		try {
			polystore.run(this.query);	
		} catch (Exception e) {
			return "error";
		}
		
		return "index";
	}
}
