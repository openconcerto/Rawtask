package com.rawtask;

import java.security.Principal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;

import org.eclipse.jetty.security.DefaultIdentityService;
import org.eclipse.jetty.security.IdentityService;
import org.eclipse.jetty.security.LoginService;
import org.eclipse.jetty.security.MappedLoginService;
import org.eclipse.jetty.security.MappedLoginService.UserPrincipal;
import org.eclipse.jetty.server.UserIdentity;

public class GloablLoginService implements LoginService {
    protected final Map<String, UserIdentity> users = new HashMap<>();
    protected final Map<Long, String> usersById = new HashMap<>();
    private IdentityService service = new DefaultIdentityService();
    private final TaskServer server;
    private final Set<String> weakPasswords = new HashSet<>(Arrays.asList("123456", "1234567", "12345678", "123456789", "1234567890", "passw0rd", "password", "qwerty", "azerty", "azertyuiop",
            "qwertyuiop", "football", "baseball", "welcome", "abc123", "toto1234", "111111", "1qaz2wsx", "dragon", "master", "monkey", "letmein", "princess", "starwars"));

    public GloablLoginService(TaskServer server, String adminPassword) throws Exception {
        this.server = server;
        final String sql = "SELECT id,login,password FROM rawtask_user";
        try (final Statement statement = server.getDBConnection().createStatement()) {
            try (final ResultSet rs = statement.executeQuery(sql)) {
                while (rs.next()) {
                    final long id = rs.getLong(1);
                    final String username = rs.getString(2);
                    final String hash = rs.getString(3);
                    // Create identity
                    this.users.put(username, this.createIdentity(username, new PBKDF2Credential(hash)));
                    this.usersById.put(id, username);
                }
                if (adminPassword != null) {
                    TaskServer.LOGGER.warning("Using overridden admin password (see rawtask.properties)");
                    this.users.put("admin", this.createIdentity("admin", new PBKDF2Credential(PasswordHash.createHash(adminPassword))));
                    this.usersById.put(Long.valueOf(1), "admin");
                }
            }
        }
    }

    public synchronized int addUser(String username, String password) throws Exception {
        final String hash = PasswordHash.createHash(password);
        final UserIdentity identity = this.createIdentity(username, new PBKDF2Credential(hash));

        //
        final String sql = "INSERT INTO rawtask_user (login,password) VALUES (?,?)";
        final PreparedStatement preparedStatement = this.server.getDBConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        preparedStatement.setString(1, username);
        preparedStatement.setString(2, hash);
        preparedStatement.executeUpdate();
        final ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
        if (generatedKeys.next()) {
            final int id = (int) generatedKeys.getLong(1);
            preparedStatement.close();
            this.usersById.put((long) id, username);
            this.users.put(username, identity);
            TaskServer.LOGGER.info("New user added, id : " + id);
            return id;
        }
        return -1;
    }

    private UserIdentity createIdentity(String username, PBKDF2Credential credential) {
        final Principal userPrincipal = new MappedLoginService.KnownUser(username, credential);
        final Subject subject = new Subject();
        subject.getPrincipals().add(userPrincipal);
        subject.getPrivateCredentials().add(credential);
        subject.getPrincipals().add(new MappedLoginService.RolePrincipal(username));
        subject.setReadOnly();
        String role = "user";
        if (username.equals("admin")) {
            role = "admin";
        }
        return this.service.newUserIdentity(subject, userPrincipal, new String[] { role });
    }

    public synchronized void deleteUser(Long id) throws Exception {
        final String username = this.usersById.get(id);
        if (username == null) {
            TaskServer.LOGGER.info("Unknown user id " + id + ".");
            return;
        }
        if (username.equals("admin")) {
            TaskServer.LOGGER.info("Attempt to delete admin user rejected.");
            return;
        }
        final String sql = "DELETE FROM rawtask_user WHERE id=" + id;
        TaskServer.LOGGER.info("Deleting user id : " + id);
        final Statement statement = this.server.getDBConnection().createStatement();
        statement.executeUpdate(sql);
        statement.close();
        this.usersById.remove(id);
        this.users.remove(username);
    }

    @Override
    public synchronized IdentityService getIdentityService() {
        return this.service;
    }

    @Override
    public String getName() {
        return "GlobalLoginService";
    }

    public int getUserCount() {
        return this.users.size();
    }

    public String getUserFromId(long id) {
        return this.usersById.get(id);
    }

    public boolean isLoginAllowedForUpdate(String login) {
        if (login == null || login.length() < 3) {
            return false;
        }
        if (login.equalsIgnoreCase("admin")) {
            return false;
        }
        return !this.users.keySet().contains(login);
    }

    public boolean isPasswordAllowed(String password) {
        if (password == null || password.length() < 6) {
            return false;
        }
        if (this.users.keySet().contains(password)) {
            return false;
        }
        return !this.weakPasswords.contains(password);
    }

    @Override
    public synchronized UserIdentity login(String username, Object credentials) {
        final UserIdentity user = this.users.get(username);
        if (user != null) {
            final UserPrincipal principal = (UserPrincipal) user.getUserPrincipal();

            if (principal.authenticate(credentials)) {
                return user;
            }

        }
        return null;
    }

    @Override
    public void logout(UserIdentity user) {
        // nothing to do
    }

    @Override
    public synchronized void setIdentityService(IdentityService service) {
        this.service = service;
    }

    public void updateUserLogin(long id, String newlogin) {
        final String login = this.usersById.get(id);
        TaskServer.LOGGER.info("Updating login for user id : " + id + " : " + login + " to " + newlogin);
        final String sql = "UPDATE rawtask_user SET login=?   WHERE id=" + id;
        PreparedStatement preparedStatement = null;
        try {
            preparedStatement = this.server.getDBConnection().prepareStatement(sql);
            preparedStatement.setString(1, newlogin);
            preparedStatement.executeUpdate();
        } catch (final SQLException e) {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (final SQLException e1) {
                    e1.printStackTrace();
                }
            }
        }

        final Subject subject = this.users.get(login).getSubject();
        final Set<Object> privateCredentials = subject.getPrivateCredentials();
        final PBKDF2Credential cred = (PBKDF2Credential) privateCredentials.iterator().next();
        this.users.remove(login);
        this.usersById.put(id, newlogin);
        final UserIdentity identity = this.createIdentity(newlogin, cred);
        this.users.put(newlogin, identity);

    }

    public synchronized void updateUserPassword(Long id, String password) throws Exception {
        final String hash = PasswordHash.createHash(password);
        final String sql = "UPDATE rawtask_user SET  password=?  WHERE id=" + id;
        PreparedStatement preparedStatement = null;
        TaskServer.LOGGER.info("Updating password for user id : " + id);
        try {
            preparedStatement = this.server.getDBConnection().prepareStatement(sql);

            preparedStatement.setString(1, hash);
            preparedStatement.executeUpdate();

        } catch (final SQLException e) {
            if (preparedStatement != null) {
                try {
                    preparedStatement.close();
                } catch (final SQLException e1) {
                    e1.printStackTrace();
                }
            }
        }
        final String login = this.usersById.get(id);
        this.users.remove(login);
        final UserIdentity identity = this.createIdentity(login, new PBKDF2Credential(hash));
        this.users.put(login, identity);
    }

    @Override
    public synchronized boolean validate(UserIdentity user) {
        return this.users.containsValue(user);
    }

}
