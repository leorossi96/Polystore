package it.uniroma3.controller;



import it.uniroma3.model.facade.FacadeCypher;
import it.uniroma3.model.facade.FacadeSQL;

import javax.servlet.http.HttpServletRequest;





public class Action {

	public String execute(HttpServletRequest request) throws Exception {
		String querySQL = request.getParameter("querySQL");
		//String queryMongoDB = request.getParameter("queryMongoDB");
		String queryCypher = request.getParameter("queryCypher");

		if(!querySQL.isEmpty()){ //refactoring con classe specifica
			FacadeSQL facadeSQL = new FacadeSQL();
			String risultato = facadeSQL.gestisciQuery(querySQL);
			request.setAttribute("result", risultato);
			return "/index.jsp";
		}

		/*else {
			if (!queryMongoDB.isEmpty()){
				List<String> documenti = facade.interrogaMongoDB(queryMongoDB);
				request.setAttribute("resultMongo",documenti);
				return "/stampaMongoDB.jsp";
			}*/
			else{
				FacadeCypher facadeCypher = new FacadeCypher();
				String risultato = facadeCypher.gestisciQuery(queryCypher);
				request.setAttribute("result", risultato);
				return "/index.jsp";
			}
		}




	}




