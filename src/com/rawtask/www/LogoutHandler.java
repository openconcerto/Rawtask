package com.rawtask.www;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

public class LogoutHandler extends AbstractHandler {

	@Override
	public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		if (target.equals("/rawtask/logout")) {
			response.setContentType("text/html; charset=utf-8");
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
			baseRequest.setHandled(true);
		}
	}

}
