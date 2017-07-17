package it.uniroma3.controller;


import javax.servlet.RequestDispatcher;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

/**
 * Controller pattern MVC. E' una servlet responsabile di gestire la richiesta data in input nella form
 * @author micheletedesco1
 *
 */
@WebServlet("/controllerQuery")
public class ControllerQuery extends HttpServlet {
    private static final long serialVersionUID = 1L;

    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {


        Helper helper = new Helper();
        Action action = new Action();

        String nextPage = "/index.jsp";
        if (helper.validate(request)) {
            try {
				nextPage = action.execute(request);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

        }

        ServletContext application = getServletContext();
        RequestDispatcher rd = application.getRequestDispatcher(nextPage);
        rd.forward(request, response);
    }
}