<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib prefix="f" uri="http://java.sun.com/jsf/core"%>
<%@ taglib prefix="h" uri="http://java.sun.com/jsf/html"%>


<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html
xmlns:f="http://java.sun.com/jsf/core"
    xmlns:h="http://java.sun.com/jsf/html">
    
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>Polystore</title>
</head>
<body>
	<f:view>
		<h:form>
			<div id="login-overlay" class="modal-dialog">
				<div class="modal-content">
					<div class="modal-header">
						<h4 class="modal-title" id="myModalLabel">Query</h4>
					</div>
					<div class="modal-body">
						<div class="row">
							<div class="col-xs-6">
								<div class="well">
									<div class="form-group">
										<label for="username" class="control-label">Query </label>
										<div>
											<h:inputText value="#{queryController.query}" required="true"
												requiredMessage="Query is mandatory" id="query" />
											<h:message for="query" />
											<span class="help-block"></span>
										</div>
									</div>

									<h:commandButton action="#{queryController.executeQuery}"
										value="Execute">
									</h:commandButton>
								</div>
							</div>
						</div>
					</div>
				</div>
			</div>
		</h:form>
	</f:view>
</body>
</html>