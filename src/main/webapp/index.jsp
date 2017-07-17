<%@ page language="java" contentType="text/html; charset=UTF-8"
         pageEncoding="UTF-8" %>
<%@taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ page isELIgnored="false" %>
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta http-equiv="X-UA-Compatible" content="IE=edge">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Example of Bootstrap 3 Static Navbar Extended</title>
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap.min.css">
<link rel="stylesheet" href="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/css/bootstrap-theme.min.css">
<script src="https://ajax.googleapis.com/ajax/libs/jquery/1.12.4/jquery.min.js"></script>
<script src="https://maxcdn.bootstrapcdn.com/bootstrap/3.3.7/js/bootstrap.min.js"></script>
<style type="text/css">
    .bs-example{
    	margin: 20px;
    }
</style>
</head> 
<body>
<div class="row affix-row">
    <div class="col-sm-3 col-md-2 affix-sidebar">
		<div class="sidebar-nav">
  <div class="navbar navbar-default" role="navigation">
    <div class="navbar-header">
      <button type="button" class="navbar-toggle" data-toggle="collapse" data-target=".sidebar-navbar-collapse">
      <span class="sr-only">Toggle navigation</span>
      <span class="icon-bar"></span>
      <span class="icon-bar"></span>
      <span class="icon-bar"></span>
      </button>
      <span class="visible-xs navbar-brand">Sidebar menu</span>
    </div>
    <div class="navbar-collapse collapse sidebar-navbar-collapse">
      <ul class="nav navbar-nav" id="sidenav01">
        <li class="active">
          <h4>
          DATABASE
          <br>
          </h4>
          
        </li>
        <li>
          <a href="#" data-toggle="collapse" data-target="#toggleDemo" data-parent="#sidenav01" class="collapsed">
          <span class="glyphicon glyphicon-floppy-disk"></span> PostgreSQL <span class="caret pull-right"></span>
          </a>
          <div class="collapse" id="toggleDemo" style="height: 0px;">
            <ul class="nav nav-list">
              <li><a href="#" style="color:blue" data-toggle="collapse" data-target="#toggleDemo4" data-parent="#sidenav01" class="collapsed"> customer <span class="caret pull-right"></span></a><div class="collapse" id="toggleDemo4" style="height: 0px;">
              <ul class="nav nav-list">
                  <li><div align="right"><a href="#" style="color:black">customer_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">store_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">first_name</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">last_name</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">email</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">address_id</a></div></li>
              </ul>
                  </div>
                  </li>
                  <li><a href="#" style="color:blue" data-toggle="collapse" data-target="#toggleDemo5" data-parent="#sidenav01" class="collapsed"> address <span class="caret pull-right"></span></a><div class="collapse" id="toggleDemo5" style="height: 0px;">
              <ul class="nav nav-list">
                  <li><div align="right"><a href="#" style="color:black">address_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">address</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">address2</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">district</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">city_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">postal_code</a></div></li>
              </ul>
                  </div>
                  </li>
                  <li><a href="#" style="color:blue" data-toggle="collapse" data-target="#toggleDemo6" data-parent="#sidenav01" class="collapsed"> city <span class="caret pull-right"></span></a><div class="collapse" id="toggleDemo6" style="height: 0px;">
              <ul class="nav nav-list">
                  <li><div align="right"><a href="#" style="color:black">city_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">city</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">country_id</a></div></li>
              </ul>
                  </div>
                  </li>
                  <li><a href="#" style="color:blue" data-toggle="collapse" data-target="#toggleDemo7" data-parent="#sidenav01" class="collapsed"> country <span class="caret pull-right"></span></a><div class="collapse" id="toggleDemo7" style="height: 0px;">
              <ul class="nav nav-list">
                  <li><div align="right"><a href="#" style="color:black">country_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">country</a></div></li>
              </ul>
                  </div>
                  </li>
            </ul>
          </div>
        </li>
        <li>
          <a href="#" data-toggle="collapse" data-target="#toggleDemo3" data-parent="#sidenav01" class="collapsed">
          <span class="glyphicon glyphicon-floppy-disk"></span> Neo4J <span class="caret pull-right"></span>
          </a>
          <div class="collapse" id="toggleDemo3" style="height: 0px;">
            <ul class="nav nav-list">
              <li><a href="#" style="color:blue" data-toggle="collapse" data-target="#toggleDemo8" data-parent="#sidenav01" class="collapsed"> store <span class="caret pull-right"></span></a><div class="collapse" id="toggleDemo8" style="height: 0px;">
              <ul class="nav nav-list">
                  <li><div align="right"><a href="#" style="color:black">store_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">manager_staff_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">address_id</a></div></li>
              </ul>
                  </div>
                  </li>
                  <li><a href="#" style="color:blue" data-toggle="collapse" data-target="#toggleDemo9" data-parent="#sidenav01" class="collapsed"> staff <span class="caret pull-right"></span></a><div class="collapse" id="toggleDemo9" style="height: 0px;">
              <ul class="nav nav-list">
                  <li><div align="right"><a href="#" style="color:black">staff_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">first_name</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">last_name</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">address_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">email</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">store_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">active</a></div></li>
              </ul>
                  </div>
                  </li>
                  <li><a href="#" style="color:blue" data-toggle="collapse" data-target="#toggleDemo10" data-parent="#sidenav01" class="collapsed"> payment <span class="caret pull-right"></span></a><div class="collapse" id="toggleDemo10" style="height: 0px;">
              <ul class="nav nav-list">
                  <li><div align="right"><a href="#" style="color:black">payment_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">customer_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">staff_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">rental_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">amount</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">payment_date</a></div></li>
              </ul>
                  </div>
                  </li>
                  <li><a href="#" style="color:blue" data-toggle="collapse" data-target="#toggleDemo11" data-parent="#sidenav01" class="collapsed"> rental <span class="caret pull-right"></span></a><div class="collapse" id="toggleDemo11" style="height: 0px;">
              <ul class="nav nav-list">
                  <li><div align="right"><a href="#" style="color:black">rental_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">staff_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">rental_date</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">customer_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">inventory_id</a></div></li>
              </ul>
                  </div>
                  </li>
            </ul>
          </div>
        </li>
        <!--<li class="active">-->
        <li>
          <a href="#" data-toggle="collapse" data-target="#toggleDemo2" data-parent="#sidenav01" class="collapsed">
          <span class="glyphicon glyphicon-floppy-disk"></span> MongoDB <span class="caret pull-right"></span>
          </a>
          <div class="collapse" id="toggleDemo2" style="height: 0px;">
            <ul class="nav nav-list">
            <li><a href="#" style="color:blue" data-toggle="collapse" data-target="#toggleDemo16" data-parent="#sidenav01" class="collapsed"> inventory <span class="caret pull-right"></span></a><div class="collapse" id="toggleDemo16" style="height: 0px;">
              <ul class="nav nav-list">
                  <li><div align="right"><a href="#" style="color:black">inventory_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">film_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">store_id</a></div></li>
              </ul>
                  </div>
                  </li>
              <li><a href="#" style="color:blue" data-toggle="collapse" data-target="#toggleDemo17" data-parent="#sidenav01" class="collapsed"> film <span class="caret pull-right"></span></a><div class="collapse" id="toggleDemo17" style="height: 0px;">
              <ul class="nav nav-list">
                  <li><div align="right"><a href="#" style="color:black">film_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">title</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">release_year</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">language_id</a></div></li>
                  <li><div align="right"><a href="#" style="color:black">original_language</a></div></li>
              </ul>
                  </div>
                  </li>
            </ul>
          </div>
        </li>
      </ul>
      </div><!--/.nav-collapse -->
    </div>
  </div>
	</div>
	<div class="col-sm-9 col-md-10 affix-content">
		<div class="container">
			
				<div class="page-header">
	<h3><i> Inserisci query nel linguaggio che conosci </i></h3>
	
</div>
<i>Esempio: <font color="red">SELECT * FROM customer </font> oppure <font color="blue">db.customer.find({})</font> oppure <font color="green">MATCH(customer:customer) RETURN customer.*</font> </i>
<div class="form_container">
		<form action="controllerQuery" method="post">
					<div class="form-group">
						<label>Query </label> <input type="text" class="form-control" name="query">
					</div>
					<button type="submit" class="btn btn-default">Invia</button>
					<p>
					${queryError}
					</p>
				</form>
			</div>
</div>
<div class="panel-heading">${query}</div>
<table class="table table-bordered" style="width :100%">
                <thead>
                <tr>
                    <c:forEach var="attributo" items="${attributi}">
                        <th>${attributo}</th>
                    </c:forEach>
                </tr>
                </thead>
                <tbody>
                <c:forEach var="riga" items="${matriceRisultati}" >
                    <tr>
                        <c:forEach items="${riga}" var="elemento">
                            <td>${elemento}</td>
                        </c:forEach>
                    </tr>
                </c:forEach>

                </tbody>
            </table>
	</div>
</div>

</body>
</html>                          