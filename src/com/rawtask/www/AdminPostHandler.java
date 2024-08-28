package com.rawtask.www;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import com.rawtask.GloablLoginService;
import com.rawtask.TaskServer;

public class AdminPostHandler extends AbstractHandler {

    private final TaskServer server;
    private final List<String> editableTables = Arrays.asList("component", "milestone", "version");
    private final GloablLoginService loginService;

    public AdminPostHandler(TaskServer server, GloablLoginService loginService) {
        this.server = server;
        this.loginService = loginService;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        if (!baseRequest.getMethod().equals("POST") || !target.equals("/rawtask/update")) {
            return;
        }
        if (!"admin".equals(baseRequest.getUserPrincipal().getName())) {
            return;
        }

        response.setContentType("text/html; charset=utf-8");
        response.setStatus(HttpServletResponse.SC_OK);
        final InputStream in = baseRequest.getInputStream();
        final String req = TaskServer.readUTF8String(in, 1024);

        final String[] parts = req.split("&");
        final Map<String, String> params = new HashMap<>();
        for (int i = 0; i < parts.length; i++) {
            final String string = parts[i];
            final int index = string.indexOf('=');
            if (index > 0 && index <= string.length() - 1) {
                String value = string.substring(index + 1, string.length());
                value = URLDecoder.decode(value, "UTF8");
                params.put(string.substring(0, index), value);
            }
        }

        if ("add".equals(params.get("action"))) {
            final String name = params.get("name");
            if ("user".equals(name)) {
                final Random r = new Random();
                int newId = -1;
                try {
                    newId = this.loginService.addUser("user" + (this.loginService.getUserCount() + 1), "temporary-password" + r.nextInt());
                } catch (final Exception e) {
                    e.printStackTrace();
                }
                response.getOutputStream().write(String.valueOf(newId).getBytes());
            } else if (this.editableTables.contains(name)) {
                final String sql = "INSERT INTO " + name + " (name) VALUES (?)";

                PreparedStatement preparedStatement;
                try {
                    preparedStatement = this.server.getDBConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                    preparedStatement.setString(1, "new " + name);
                    preparedStatement.executeUpdate();
                    final ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
                    if (generatedKeys.next()) {
                        final int id = (int) generatedKeys.getLong(1);
                        preparedStatement.close();
                        final String newId = String.valueOf(id);
                        TaskServer.LOGGER.info("New " + name + " added, id : " + newId);
                        response.getOutputStream().write(newId.getBytes());
                    }
                } catch (final SQLException e) {
                    e.printStackTrace();
                }
            }
        } else if ("remove".equals(params.get("action"))) {
            // delete
            final String name = params.get("name");
            if ("user".equals(name)) {
                final long id = Long.parseLong(params.get("pk"));

                try {
                    this.loginService.deleteUser(id);
                } catch (final Exception e) {
                    e.printStackTrace();
                }

            } else if (this.editableTables.contains(name)) {
                final long id = Long.parseLong(params.get("pk"));
                final String sql = "DELETE FROM " + name + " WHERE id=" + id;
                TaskServer.LOGGER.info("Deleting " + name + " id : " + id);
                try {
                    final Statement statement = this.server.getDBConnection().createStatement();
                    statement.executeUpdate(sql);
                    statement.close();

                } catch (final SQLException e) {
                    e.printStackTrace();
                }
            }
        } else {
            // updates
            final String name = params.get("name");
            if ("password".equals(name)) {
                final String value = params.get("value");
                final long id = Long.parseLong(params.get("pk"));
                if (this.loginService.isPasswordAllowed(value)) {
                    try {
                        this.loginService.updateUserPassword(id, value);
                        response.getOutputStream().write("{\"success\":true,\"msg\":\"Password updated\"}".getBytes());
                    } catch (final Exception e) {
                        TaskServer.LOGGER.log(Level.SEVERE, " cannot change password " + value, e);
                    }
                } else {
                    TaskServer.LOGGER.info("Invalid password " + value);
                    response.getOutputStream().write("{\"success\":false,\"msg\":\"Invalid password\"}".getBytes());
                }

            } else if ("login".equals(name)) {
                final String value = params.get("value");
                final long id = Long.parseLong(params.get("pk"));
                if (this.loginService.isLoginAllowedForUpdate(value)) {
                    try {
                        this.loginService.updateUserLogin(id, value);
                        response.getOutputStream().write("{\"success\":true,\"msg\":\"Login updated\"}".getBytes());
                    } catch (final Exception e) {
                        TaskServer.LOGGER.log(Level.SEVERE, " cannot update login " + value, e);
                    }
                } else {
                    TaskServer.LOGGER.info("Invalid login " + value);
                    response.getOutputStream().write("{\"success\":false,\"msg\":\"Invalid login\"}".getBytes());
                }

            } else if (this.editableTables.contains(name)) {
                final long id = Long.parseLong(params.get("pk"));
                final String value = params.get("value");

                final String sql = "UPDATE " + name + " SET name=?   WHERE id=" + id;
                PreparedStatement preparedStatement = null;
                try {
                    preparedStatement = this.server.getDBConnection().prepareStatement(sql);
                    preparedStatement.setString(1, value);
                    preparedStatement.executeUpdate();
                } catch (final SQLException e) {
                    if (preparedStatement != null) {
                        try {
                            preparedStatement.close();
                        } catch (final SQLException e1) {
                            TaskServer.LOGGER.log(Level.SEVERE, " cannot update table rawtask_user " + value, e);
                        }
                    }
                }
            }
        }

        baseRequest.setHandled(true);

    }

}
