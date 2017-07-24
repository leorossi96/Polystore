  package it.uniroma3.controller;


  import javax.servlet.http.HttpServletRequest;

  public class Helper {
      boolean validate(HttpServletRequest request) {

          String query = request.getParameter("query");
          Boolean corretto = true;

          if (query.equals("")) {
              corretto = false;
              request.setAttribute("queryError", "almeno una query è richiesta");

          }
          return corretto;
      }
  }