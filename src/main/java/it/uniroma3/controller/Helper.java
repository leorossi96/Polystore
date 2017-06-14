  package it.uniroma3.controller;


  import javax.servlet.http.HttpServletRequest;

  public class Helper {
      boolean validate(HttpServletRequest request) {

          String querySQL = request.getParameter("querySQL");
          String queryMongoDB = request.getParameter("queryMongoDB");
          String queryCypher = request.getParameter("queryCypher");
          Boolean corretto = true;

          if (querySQL.equals("") && queryCypher.equals("") && queryMongoDB.equals("") ) {
              corretto = false;
              request.setAttribute("queryError", "almeno una query è richiesta");

          }
          return corretto;
      }
  }
