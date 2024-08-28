package com.rawtask.www;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.rawtask.TaskServer;
import com.rawtask.sql.SQLFetcher;

import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;
import freemarker.template.TemplateExceptionHandler;

public class UserAdminHandler extends AbstractHandler {

    private final TaskServer server;

    public UserAdminHandler(TaskServer server) {
        this.server = server;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        if (!baseRequest.getMethod().equals("GET")) {
            return;
        }
        if (!target.equals("/rawtask/users.html")) {
            return;
        }
        if (!"admin".equals(baseRequest.getUserPrincipal().getName())) {
            response.setContentType("text/html; charset=utf-8");
            response.setStatus(HttpServletResponse.SC_OK);
            final PrintWriter out = response.getWriter();
            out.write("<html><head/><body><h2>Only admin user is allowed to access this page.</h2><br/><br/><i>Powered by <a href=\"http://www.rawtask.com\">Rawtask.</a></i></body></html>");

            out.close();
            baseRequest.setHandled(true);
            return;
        }

        response.setContentType("text/html; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);

        final Configuration cfg = new Configuration(Configuration.VERSION_2_3_24);
        cfg.setDirectoryForTemplateLoading(new File("resources"));
        cfg.setDefaultEncoding("UTF-8");
        cfg.setTemplateExceptionHandler(TemplateExceptionHandler.RETHROW_HANDLER);
        cfg.setLogTemplateExceptions(false);

        final Map<String, Object> root = new HashMap<>();
        try {
            final SQLFetcher f = new SQLFetcher(this.server.getDBConnection());
            root.put("users", f.fetchRecordFromTable("rawtask_user", "login"));
        } catch (final SQLException e1) {
            TaskServer.LOGGER.log(Level.SEVERE, "cannot fetch data", e1);
        }
        // Get the template (uses cache internally)
        final Template temp = cfg.getTemplate("users.html");

        // Merge data-model with template
        final PrintWriter out = response.getWriter();
        try {
            temp.process(root, out);
        } catch (final TemplateException e) {
            TaskServer.LOGGER.log(Level.SEVERE, "cannot fill template", e);
        }

        out.close();
        baseRequest.setHandled(true);

    }

}
