package com.rawtask.xmlrpc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.jdom2.Document;
import org.jdom2.input.SAXBuilder;
import org.jdom2.input.sax.XMLReaders;

import com.rawtask.ByteArray;
import com.rawtask.Change;
import com.rawtask.TaskServer;
import com.rawtask.sql.QueryParser;
import com.rawtask.sql.SQLFetcher;

public class XMLRpcHandler extends AbstractHandler {

    private static final String STATUS_NEW = "new";
    final List<String> status = new ArrayList<String>(5);
    final List<String> types = new ArrayList<String>(3);
    final List<String> priorities = new ArrayList<String>(5);
    final List<String> severities = new ArrayList<String>(5);
    final List<String> resolutions = new ArrayList<String>(5);
    final Set<String> ticketFields = new HashSet<String>(Arrays.asList("type", "time", "changetime", "component", "severity", "priority", "owner", "reporter", "cc", "version", "milestone", "status",
            "resolution", "summary", "description", "keywords"));
    private final TaskServer server;

    public XMLRpcHandler(TaskServer server) {
        this.server = server;
        // define default values
        this.status.add("accepted");
        this.status.add("assigned");
        this.status.add("closed");
        this.status.add(XMLRpcHandler.STATUS_NEW);
        this.status.add("reopened");
        this.types.add("defect");
        this.types.add("enhancement");
        this.types.add("task");
        this.priorities.add("very high");
        this.priorities.add("high");
        this.priorities.add("normal");
        this.priorities.add("low");
        this.priorities.add("very low");
        this.resolutions.add("fixed");
        this.resolutions.add("invalid");
        this.resolutions.add("wontfix");
        this.resolutions.add("duplicate");
        this.resolutions.add("worksforme");
        this.severities.add("blocker");
        this.severities.add("critical");
        this.severities.add("major");
        this.severities.add("minor");
        this.severities.add("trivial");
    }

    Map<String, Object> create(String type, String name, String label) {
        final Map<String, Object> m1 = new HashMap<>();
        m1.put("type", type);
        m1.put("name", name);
        m1.put("label", label);
        return m1;
    }

    private List<String> getComponentsNames() throws SQLException {
        final SQLFetcher fetcher = new SQLFetcher(this.server.getDBConnection());
        final List<String> components = fetcher.fetchColumnFromTable("component", "name");
        Collections.sort(components);
        return components;
    }

    private List<String> getMilestonesNames() throws SQLException {
        final SQLFetcher fetcher = new SQLFetcher(this.server.getDBConnection());
        final List<String> milestones = fetcher.fetchColumnFromTable("milestone", "name");
        Collections.sort(milestones);
        return milestones;
    }

    private File getPreviousFile(Integer id, String fileName) throws SQLException {
        final String sql = "SELECT file FROM attachment WHERE ticket_id=?  AND filename=? ORDER BY time DESC LIMIT 1 ";
        final PreparedStatement statement = this.server.getDBConnection().prepareStatement(sql);
        statement.setInt(1, id);
        statement.setString(2, fileName);
        final ResultSet rs = statement.executeQuery();
        String file = null;
        while (rs.next()) {
            file = rs.getString(1);
        }
        statement.close();
        if (file != null) {
            final File f = new File(getAttachmentDir(id) + "/", file);
            if (f.exists()) {
                TaskServer.LOGGER.finest("Previous file : " + f.getAbsolutePath());
                return f;
            } else {
                TaskServer.LOGGER.severe("Previous file missing : " + f.getAbsolutePath());
            }
        }
        TaskServer.LOGGER.finest("No previous file found");
        return null;
    }

    private File getAttachmentDir(Integer id) {
        return new File(this.server.getDataDir(), "attachments/" + id);
    }

    private List<String> getVersionsNames() throws SQLException {
        final SQLFetcher fetcher = new SQLFetcher(this.server.getDBConnection());
        final List<String> components = fetcher.fetchColumnFromTable("version", "name");
        Collections.sort(components);
        return components;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
        if (baseRequest.getMethod().equals("POST")) {
            final String contentType = baseRequest.getContentType();
            if ("text/xml".equals(contentType) || "application/xml".equals(contentType)) {
                final InputStream in = baseRequest.getInputStream();
                final String xml = TaskServer.readUTF8String(in, 1024);
                final SAXBuilder saxBuilder = new SAXBuilder();
                saxBuilder.setXMLReaderFactory(XMLReaders.NONVALIDATING);
                saxBuilder.setExpandEntities(false);
                saxBuilder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
                saxBuilder.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
                saxBuilder.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
                saxBuilder.setFeature("http://xml.org/sax/features/external-general-entities", false);
                saxBuilder.setFeature("http://xml.org/sax/features/resolve-dtd-uris", false);
                saxBuilder.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
                final String user = baseRequest.getUserPrincipal().getName();
                Document doc;
                try {
                    doc = saxBuilder.build(new StringReader(xml));
                    final boolean multicall = doc.getRootElement().getChildText("methodName").equals("system.multicall");
                    final List<XMLRpcCall> calls = XMLRpc.parse(doc.getRootElement());
                    final List<Object> results = new ArrayList<Object>();
                    for (final XMLRpcCall xmlRpcCall : calls) {
                        final String methodName = xmlRpcCall.getMethod();
                        if (methodName.equals("system.getAPIVersion")) {
                            results.add(this.handleGetAPIversion());
                        } else if (methodName.equals("ticket.component.getAll")) {
                            results.add(this.handleGetAllTicketComponent());
                        } else if (methodName.equals("ticket.component.get")) {
                            results.add(this.handleGetTicketComponent(xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.milestone.getAll")) {
                            results.add(this.handleGetAllTicketMilestone());
                        } else if (methodName.equals("ticket.milestone.get")) {
                            results.add(this.handleGetTicketMilestone(xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.priority.getAll")) {
                            results.add(this.handleGetAllTicketPriorities());
                        } else if (methodName.equals("ticket.priority.get")) {
                            results.add(this.handleGetTicketPriority(xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.resolution.getAll")) {
                            results.add(this.handleGetAllTicketResolutions());
                        } else if (methodName.equals("ticket.resolution.get")) {
                            results.add(this.handleGetTicketResolution(xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.severity.getAll")) {
                            results.add(this.handleGetAllTicketSeverities());
                        } else if (methodName.equals("ticket.severity.get")) {
                            results.add(this.handleGetTicketSeverity(xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.status.getAll")) {
                            results.add(this.handleGetAllTicketStatus());
                        } else if (methodName.equals("ticket.status.get")) {
                            results.add(this.handleGetTicketStatus(xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.type.getAll")) {
                            results.add(this.handleGetAllTicketTypes());
                        } else if (methodName.equals("ticket.type.get")) {
                            results.add(this.handleGetTicketType(xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.version.getAll")) {
                            results.add(this.handleGetAllTicketVersions());
                        } else if (methodName.equals("ticket.version.get")) {
                            results.add(this.handleGetTicketVersion(xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.getTicketFields")) {
                            results.add(this.handleGetTicketFields());
                        } else if (methodName.equals("ticket.create")) {
                            results.add(this.handleCreateTicket(user, xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.delete")) {
                            results.add(this.handleDeleteTicket(user, xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.get")) {
                            results.add(this.handleGetTicket(xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.changeLog")) {
                            results.add(this.handleGetTicketChangeLog(xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.listAttachments")) {
                            results.add(this.handleListTicketAttachements(xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.getActions")) {
                            results.add(this.handleGetTicketActions(user, xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.update")) {
                            results.add(this.handleUpdateTicket(user, xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.getRecentChanges")) {
                            results.add(this.handleGetRecentTicketChanges(xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.query")) {
                            results.add(this.handleTicketQuery(xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.putAttachment")) {
                            results.add(this.handlePutTicketAttachment(user, xmlRpcCall.getParams()));
                        } else if (methodName.equals("ticket.getAttachment")) {
                            results.add(this.handleGetTicketAttachment(xmlRpcCall.getParams()));
                        } else {
                            throw new IllegalArgumentException("unknown method " + methodName);
                        }
                    }
                    String result = "";
                    try {
                        if (!multicall) {
                            result = XMLRpc.createResponse(results.get(0));
                        } else {
                            List<Object> r = new ArrayList<>(results.size());
                            for (Object object : results) {
                                List<Object> l = new ArrayList<>(1);
                                l.add(object);
                                r.add(l);
                            }
                            result = XMLRpc.createResponse(r);
                        }
                        response.setStatus(HttpServletResponse.SC_OK);
                    } catch (Exception e) {
                        e.printStackTrace();
                        TaskServer.LOGGER.severe("Unable to create response for : " + xml);
                        TaskServer.LOGGER.severe("Result (multicall:" + multicall + ") : " + results);
                        response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
                    }
                    response.setContentType(contentType + "; charset=utf-8");

                    final PrintWriter out = response.getWriter();
                    out.println(result);

                } catch (final Exception e) {
                    e.printStackTrace();
                }

                baseRequest.setHandled(true);
            }
        }

    }

    private synchronized Object handleCreateTicket(String user, List<?> params) throws SQLException {
        final String summary = (String) params.get(0);
        final String description = (String) params.get(1);
        @SuppressWarnings("unchecked")
        final Map<String, Object> attributes = (Map<String, Object>) params.get(2);
        final String severity = (String) attributes.get("severity");
        final String cc = (String) attributes.get("cc");
        final String owner = (String) attributes.get("owner");
        final String component = (String) attributes.get("component");
        final String milestone = (String) attributes.get("milestone");
        final String keywords = (String) attributes.get("keywords");
        attributes.get("action");
        final String type = (String) attributes.get("type");
        final String priority = (String) attributes.get("priority");
        final String version = (String) attributes.get("version");
        final long time = System.currentTimeMillis();
        final long changetime = System.currentTimeMillis();
        params.get(3);
        if (params.size() > 4) {
            // TODO : DateTime when=None
        }
        PreparedStatement statement = this.server.getDBConnection().prepareStatement("SELECT MAX(id) FROM ticket;");
        ResultSet rs = statement.executeQuery();
        rs.next();
        int id = rs.getInt(1) + 1;

        final String sql = "INSERT INTO ticket (type,time,changetime,component,severity,priority,owner,reporter,cc,version,milestone,status,resolution,summary,description,keywords,id) VALUES ( ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        final PreparedStatement preparedStatement = this.server.getDBConnection().prepareStatement(sql);
        preparedStatement.setString(1, type);
        preparedStatement.setLong(2, time);
        preparedStatement.setLong(3, changetime);
        preparedStatement.setString(4, component);
        preparedStatement.setString(5, severity);
        preparedStatement.setString(6, priority);
        preparedStatement.setString(7, owner);
        final String reporter = user;
        preparedStatement.setString(8, reporter);
        preparedStatement.setString(9, cc);
        preparedStatement.setString(10, version);
        preparedStatement.setString(11, milestone);
        preparedStatement.setString(12, XMLRpcHandler.STATUS_NEW);
        final String resolution = "";
        preparedStatement.setString(13, resolution);
        preparedStatement.setString(14, summary);
        preparedStatement.setString(15, description);
        preparedStatement.setString(16, keywords);
        //

        preparedStatement.setInt(17, id);
        preparedStatement.executeUpdate();
        TaskServer.LOGGER.fine("Ticket " + id + " created by " + user + ".");
        return id;
    }

    private synchronized Integer handleDeleteTicket(String user, List<?> params) throws SQLException {
        final Integer requestedId = (Integer) params.get(0);
        final Statement statement = this.server.getDBConnection().createStatement();
        statement.executeUpdate("DELETE FROM ticket WHERE id=" + requestedId);
        statement.executeUpdate("DELETE FROM attachment WHERE ticket_id=" + requestedId);
        statement.executeUpdate("DELETE FROM ticket_change WHERE ticket_id=" + requestedId);
        statement.close();
        File dir = this.getAttachmentDir(requestedId);
        try {

            File[] files = dir.listFiles();
            if (files != null) {
                for (int i = 0; i < files.length; i++) {
                    boolean b = files[i].delete();
                    if (!b) {
                        TaskServer.LOGGER.severe("Cannot delete file " + files[i].getAbsolutePath());
                    }
                }
            }
        } catch (Exception e) {
            TaskServer.LOGGER.severe("Cannot delete directory content of " + dir.getAbsolutePath());
        }
        TaskServer.LOGGER.fine("Ticket " + requestedId + " deleted by " + user + ".");
        return requestedId;
    }

    private List<String> handleGetAllTicketComponent() throws SQLException {
        return this.getComponentsNames();
    }

    private List<String> handleGetAllTicketMilestone() throws SQLException {
        return this.getMilestonesNames();
    }

    private List<String> handleGetAllTicketPriorities() {
        return this.priorities;
    }

    private List<String> handleGetAllTicketResolutions() {
        return this.resolutions;
    }

    private List<String> handleGetAllTicketSeverities() {
        return this.severities;
    }

    private List<String> handleGetAllTicketStatus() {
        return this.status;
    }

    private List<String> handleGetAllTicketTypes() {
        return this.types;
    }

    private List<String> handleGetAllTicketVersions() throws SQLException {
        return this.getVersionsNames();
    }

    private List<Integer> handleGetAPIversion() throws IOException {
        final List<Integer> version = new ArrayList<Integer>(3);
        version.add(1);
        version.add(1);
        version.add(6);
        return version;
    }

    /**
     * return list of ids
     *
     * @throws SQLException
     */
    private List<Integer> handleGetRecentTicketChanges(List<?> params) throws SQLException {
        final Date afterDate = (Date) params.get(0);
        final Statement statement = this.server.getDBConnection().createStatement();
        final ResultSet rs = statement.executeQuery("SELECT id FROM ticket WHERE changetime>" + afterDate.getTime());
        final ArrayList<Integer> result = new ArrayList<Integer>();
        while (rs.next()) {
            result.add((int) rs.getLong(1));
        }
        statement.close();
        return result;
    }

    /**
     * Fetch a ticket. Returns [id, time_created, time_changed, attributes].
     *
     * @throws SQLException
     */
    private Object handleGetTicket(List<?> params) throws SQLException {
        final Integer requestedId = (Integer) params.get(0);
        final Statement statement = this.server.getDBConnection().createStatement();
        final ResultSet rs = statement.executeQuery(
                "SELECT type,time,changetime,component,severity,priority,owner,reporter,cc,version,milestone,status,resolution,summary,description,keywords FROM ticket WHERE id=" + requestedId);
        final HashMap<Object, Object> attributes = new HashMap<Object, Object>();
        attributes.put("_ts", String.valueOf(System.currentTimeMillis()));
        Date timeCreated = new Date();
        Date timeChanged = new Date();
        while (rs.next()) {
            attributes.put("type", rs.getString(1));
            timeCreated = new Date(rs.getLong(2));
            attributes.put("time", timeCreated);
            timeChanged = new Date(rs.getLong(3));
            attributes.put("changetime", timeChanged);
            attributes.put("component", rs.getString(4));
            attributes.put("severity", rs.getString(5));
            attributes.put("priority", rs.getString(6));
            attributes.put("owner", rs.getString(7));
            attributes.put("reporter", rs.getString(8));
            attributes.put("cc", rs.getString(9));
            attributes.put("version", rs.getString(10));
            attributes.put("milestone", rs.getString(11));
            attributes.put("status", rs.getString(12));
            attributes.put("resolution", rs.getString(13));
            attributes.put("summary", rs.getString(14));
            attributes.put("description", rs.getString(15));
            attributes.put("keywords", rs.getString(16));
        }
        statement.close();
        final List<Object> result = new ArrayList<>();
        result.add(requestedId);
        result.add(timeCreated);
        result.add(timeChanged);
        result.add(attributes);
        return result;
    }

    private Object handleGetTicketActions(String user, List<?> params) throws SQLException {
        final Integer requestedId = (Integer) params.get(0);
        final Statement statement = this.server.getDBConnection().createStatement();
        final ResultSet rs = statement.executeQuery("SELECT status FROM ticket WHERE id=" + requestedId);
        String status = null;
        while (rs.next()) {
            status = rs.getString(1);

        }
        statement.close();
        boolean done = false;
        if (status != null && status.equals("closed")) {
            done = true;
        }

        final ArrayList<Object> result = new ArrayList<>();
        result.add(Arrays.asList("leave", "leave", ".", new ArrayList<>()));
        final ArrayList<Object> emptyList = new ArrayList<>();
        if (!done) {
            // resolve
            final ArrayList<Object> resolveParams = new ArrayList<>();
            resolveParams.add(Arrays.asList("action_resolve_resolve_resolution", "fixed", this.resolutions));
            result.add(Arrays.asList("resolve", "resolve", "The resolution will be set. Next status will be 'closed'.", resolveParams));
            //
            final ArrayList<Object> reassignParams = new ArrayList<>();
            reassignParams.add(Arrays.asList("action_reassign_reassign_owner", user, emptyList));
            result.add(Arrays.asList("reassign", "reassign", "Next status will be 'assigned'.", reassignParams));
            //
            result.add(Arrays.asList("accept", "accept", "The owner will be changed to " + user + ". Next status will be 'accepted'.", emptyList));
        } else {

            result.add(Arrays.asList("reopen", "reopen", "The resolution will be deleted. Next status will be 'reopened'.", emptyList));

        }
        return result;
    }

    private ByteArray handleGetTicketAttachment(List<?> params) throws IOException, SQLException {
        final Integer id = (Integer) params.get(0);
        final String fileName = (String) params.get(1);
        final File f = this.getPreviousFile(id, fileName);
        final byte[] bytes = new byte[(int) f.length()];
        TaskServer.LOGGER.finest("Sending file " + fileName + " for ticket " + id + " : " + f.getAbsolutePath());
        final FileInputStream fInputStream = new FileInputStream(f);
        fInputStream.read(bytes);
        fInputStream.close();
        return new ByteArray(bytes);
    }

    private Object handleGetTicketChangeLog(List<?> params) throws SQLException {
        final Integer requestedId = (Integer) params.get(0);
        final Integer when = (Integer) params.get(1);
        final Statement statement = this.server.getDBConnection().createStatement();
        final ResultSet rs = statement.executeQuery("SELECT time, author,field,oldvalue,newvalue FROM ticket_change WHERE ticket_id=" + requestedId + " AND time>=" + when);

        final ArrayList<Object> result = new ArrayList<>();
        while (rs.next()) {
            // time, author, field,oldvalue,newvalue
            final ArrayList<Object> change = new ArrayList<>();
            change.add(new Date(rs.getLong(1)));
            change.add(rs.getString(2));
            change.add(rs.getString(3));
            change.add(rs.getString(4));
            change.add(rs.getString(5));
            result.add(change);
        }
        statement.close();
        return result;
    }

    private Map<String, String> handleGetTicketComponent(List<?> params) throws SQLException {
        final String name = (String) params.get(0);
        final Map<String, String> m1 = new HashMap<String, String>();
        final String query = "SELECT name,owner,description FROM component WHERE name=?";
        final PreparedStatement statement = this.server.getDBConnection().prepareStatement(query);
        statement.setString(1, name);
        final ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            m1.put("name", rs.getString(1));
            m1.put("owner", rs.getString(2));
            m1.put("description", rs.getString(3));
        }
        statement.close();
        return m1;
    }

    private List<Map<String, Object>> handleGetTicketFields() throws SQLException {
        final List<Map<String, Object>> l = new ArrayList<Map<String, Object>>();

        l.add(this.create("text", "summary", "Summary"));
        l.add(this.create("text", "reporter", "Reporter"));
        l.add(this.create("text", "owner", "Owner"));
        l.add(this.create("textarea", "description", "Description"));

        // Type
        final Map<String, Object> mType = new HashMap<>();
        mType.put("type", "select");
        mType.put("name", "type");
        mType.put("label", "Type");
        mType.put("options", this.types);
        mType.put("value", this.types.get(0));
        l.add(mType);
        // Status
        final Map<String, Object> mStatus = new HashMap<>();
        mStatus.put("type", "radio");
        mStatus.put("name", "status");
        mStatus.put("label", "Status");
        mStatus.put("optional", Boolean.TRUE);
        mStatus.put("options", this.status);
        mStatus.put("value", "");
        l.add(mStatus);
        // Priority
        final Map<String, Object> mPriority = new HashMap<>();
        mPriority.put("type", "select");
        mPriority.put("name", "priority");
        mPriority.put("label", "Priority");
        mPriority.put("options", this.priorities);
        mPriority.put("value", this.priorities.get(2));
        l.add(mPriority);
        // Milestone
        final Map<String, Object> mMilestone = new HashMap<>();
        mMilestone.put("type", "select");
        mMilestone.put("name", "milestone");
        mMilestone.put("label", "Milestone");
        mMilestone.put("optional", Boolean.TRUE);
        mMilestone.put("options", this.getMilestonesNames());
        mMilestone.put("value", "");
        l.add(mMilestone);
        // Component
        final Map<String, Object> mComponent = new HashMap<>();
        mComponent.put("type", "select");
        mComponent.put("name", "component");
        mComponent.put("label", "Component");
        mComponent.put("options", this.getComponentsNames());
        mComponent.put("value", "");
        l.add(mComponent);
        // Version

        final Map<String, Object> mVersion = new HashMap<>();
        mVersion.put("type", "select");
        mVersion.put("name", "version");
        mVersion.put("label", "Version");
        mVersion.put("optional", Boolean.TRUE);
        mVersion.put("options", this.getVersionsNames());
        mVersion.put("value", "");
        l.add(mStatus);
        // Resolution
        final Map<String, Object> mResolution = new HashMap<>();
        mResolution.put("type", "radio");
        mResolution.put("name", "resolution");
        mResolution.put("label", "Resolution");
        mResolution.put("optional", Boolean.TRUE);
        mResolution.put("options", this.status);
        mResolution.put("value", this.resolutions.get(0));
        l.add(mResolution);

        l.add(this.create("text", "keywords", "Keywords"));
        l.add(this.create("text", "cc", "Cc"));
        l.add(this.create("time", "time", "Created"));
        l.add(this.create("time", "changetime", "Modified"));
        return l;
    }

    private Map<String, Object> handleGetTicketMilestone(List<?> params) throws SQLException {
        final String name = (String) params.get(0);
        final Map<String, Object> m1 = new HashMap<String, Object>();
        final String query = "SELECT name,due,completed,description FROM milestone WHERE name=?";
        final PreparedStatement statement = this.server.getDBConnection().prepareStatement(query);
        statement.setString(1, name);
        final ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            m1.put("name", rs.getString(1));
            m1.put("due", (int) rs.getLong(2));
            m1.put("completed", (int) rs.getLong(3));
            m1.put("description", rs.getString(4));
        }
        statement.close();

        return m1;
    }

    private List<String> handleGetTicketPriority(List<?> params) {
        final String priority = (String) params.get(0);
        final List<String> result = new ArrayList<>(1);
        final int index = this.priorities.indexOf(priority);
        if (index >= 0) {
            result.add(String.valueOf(index + 1));
        } else {
            throw new IllegalArgumentException("unknown priority : " + priority);
        }
        return result;
    }

    private List<String> handleGetTicketResolution(List<?> params) {
        final String resolution = (String) params.get(0);
        final List<String> result = new ArrayList<>(1);
        final int index = this.resolutions.indexOf(resolution);
        if (index >= 0) {
            result.add(String.valueOf(index + 1));
        } else {
            throw new IllegalArgumentException("unknown resolution : " + resolution);
        }
        return result;
    }

    private List<String> handleGetTicketSeverity(List<?> params) {
        final String severity = (String) params.get(0);
        final List<String> result = new ArrayList<>(1);
        final int index = this.severities.indexOf(severity);
        if (index >= 0) {
            result.add(String.valueOf(index + 1));
        } else {
            throw new IllegalArgumentException("unknown severity : " + severity);
        }
        return result;
    }

    private List<String> handleGetTicketStatus(List<?> params) {
        final List<String> priority = new ArrayList<String>();
        priority.add("0");
        return priority;
    }

    private List<String> handleGetTicketType(List<?> params) {
        final List<String> result = new ArrayList<String>();
        final String type = (String) params.get(0);
        if (type.equals("defect")) {
            result.add("1");
        } else if (type.equals("enhancement")) {
            result.add("2");
        } else if (type.equals("task")) {
            result.add("3");
        } else {
            throw new IllegalArgumentException("unknown type : " + type);
        }
        return result;
    }

    private Map<String, Object> handleGetTicketVersion(List<?> params) throws SQLException {
        final String name = (String) params.get(0);
        final Map<String, Object> m1 = new HashMap<String, Object>();
        final String query = "SELECT name,time,description FROM version WHERE name=?";
        final PreparedStatement statement = this.server.getDBConnection().prepareStatement(query);
        statement.setString(1, name);
        final ResultSet rs = statement.executeQuery();
        while (rs.next()) {
            m1.put("name", rs.getString(1));
            m1.put("time", (int) rs.getLong(2));
            m1.put("description", rs.getString(3));
        }
        statement.close();

        return m1;

    }

    private Object handleListTicketAttachements(List<?> params) throws SQLException {
        final Integer id = (Integer) params.get(0);
        final Statement statement = this.server.getDBConnection().createStatement();
        final ResultSet rs = statement.executeQuery("SELECT filename,description,size,time,author FROM attachment WHERE ticket_id=" + id);
        final ArrayList<Object> result = new ArrayList<>();
        while (rs.next()) {
            final ArrayList<Object> attachment = new ArrayList<>();
            attachment.add(rs.getString(1));
            attachment.add(rs.getString(2));
            attachment.add(rs.getInt(3));
            attachment.add(new Date(rs.getLong(4)));
            attachment.add(rs.getString(5));
            result.add(attachment);
        }
        statement.close();
        return result;
    }

    private Object handlePutTicketAttachment(String user, List<?> params) throws IOException, SQLException {
        final Integer id = (Integer) params.get(0);
        final String fileName = (String) params.get(1);
        final String description = (String) params.get(2);
        final ByteArray bytes = (ByteArray) params.get(3);
        final Boolean replace = (Boolean) params.get(4);

        final File dir = getAttachmentDir(id);
        if (!dir.exists()) {
            dir.mkdirs();
        }
        final UUID uuid = UUID.randomUUID();
        String extension = ".bin";
        if (fileName.contains(".")) {
            final int index = fileName.lastIndexOf('.');
            extension = fileName.substring(index);
        }

        final String fileNameOnDisk = uuid.toString() + extension;
        final File file = new File(dir + "/", fileNameOnDisk);
        TaskServer.LOGGER.finest("Writing " + fileName + " (" + bytes.getBytes().length + " bytes) to " + file.getAbsolutePath());
        final FileOutputStream fOut = new FileOutputStream(file);
        fOut.write(bytes.getBytes());
        fOut.close();
        final File previous = this.getPreviousFile(id, fileName);

        if (previous != null) {
            if (replace) {
                previous.delete();
            }
            final Statement statement = this.server.getDBConnection().createStatement();
            final String sqlDelete = "DELETE FROM attachment WHERE id_ticket=" + id + " AND fileName='" + fileName + "'";
            statement.executeUpdate(sqlDelete);
            statement.close();
        }
        final String sql = "INSERT INTO attachment (ticket_id,filename,size,time,description,author,file) VALUES (?,?,?,?,?,?,?)";
        final PreparedStatement preparedStatement = this.server.getDBConnection().prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
        // ticket_id,filename,size,time,description,author,file
        preparedStatement.setInt(1, id);
        preparedStatement.setString(2, fileName);
        preparedStatement.setInt(3, bytes.getBytes().length);
        preparedStatement.setLong(4, System.currentTimeMillis());
        preparedStatement.setString(5, description);
        preparedStatement.setString(6, user);
        preparedStatement.setString(7, fileNameOnDisk);
        preparedStatement.executeUpdate();
        preparedStatement.close();
        TaskServer.LOGGER.fine("New attachment (" + fileName + ") added to ticket " + id + " by " + user + ".");
        return new ArrayList<>();
    }

    /**
     * Perform a ticket query, returning a list of ticket ID's. All queries will use stored settings
     * for maximum number of results per page and paging options.
     *
     * Use max=n to define number of results to receive, and use page=n to page through larger
     * result sets.
     *
     * Using max=0 will turn off paging and return all results.
     *
     * @throws SQLException
     */
    private List<Integer> handleTicketQuery(List<?> params) throws SQLException {
        final String query = (String) params.get(0);
        final String sql = QueryParser.convertToSQL(query);
        TaskServer.LOGGER.finest(sql);
        final SQLFetcher fetcher = new SQLFetcher(this.server.getDBConnection());
        return fetcher.fetchColumnId(sql);
    }

    private Object handleUpdateTicket(String user, List<?> params) throws SQLException {
        final Integer requestedId = (Integer) params.get(0);
        final String comment = (String) params.get(1);
        @SuppressWarnings("unchecked")
        final Map<String, Object> data = (Map<String, Object>) params.get(2);
        params.get(3);
        String author = user;
        if (params.size() > 4) {
            author = (String) params.get(4);
        }
        if (params.size() > 5) {
            params.get(5);
        }
        //
        final long currentTime = System.currentTimeMillis();
        // SELECT
        final List<String> keys = new ArrayList<>(data.keySet());
        final int stop = keys.size();
        final HashMap<Object, Object> attributes = new HashMap<>();
        final Statement statement = this.server.getDBConnection().createStatement();
        final ResultSet rs = statement.executeQuery(
                "SELECT type,time,changetime,component,severity,priority,owner,reporter,cc,version,milestone,status,resolution,summary,description,keywords FROM ticket WHERE id=" + requestedId);
        Date timeCreated = new Date(currentTime);

        while (rs.next()) {
            attributes.put("type", rs.getString(1));
            attributes.put("time", new Date(rs.getLong(2)));
            timeCreated = new Date(rs.getLong(2));
            attributes.put("changetime", new Date(rs.getLong(3)));
            attributes.put("component", rs.getString(4));
            attributes.put("severity", rs.getString(5));
            attributes.put("priority", rs.getString(6));
            attributes.put("owner", rs.getString(7));
            attributes.put("reporter", rs.getString(8));
            attributes.put("cc", rs.getString(9));
            attributes.put("version", rs.getString(10));
            attributes.put("milestone", rs.getString(11));
            attributes.put("status", rs.getString(12));
            attributes.put("resolution", rs.getString(13));
            attributes.put("summary", rs.getString(14));
            attributes.put("description", rs.getString(15));
            attributes.put("keywords", rs.getString(16));
        }
        statement.close();

        final List<String> modifiedKeys = new ArrayList<String>(keys.size());
        final List<Change> changeLog = new ArrayList<Change>();
        for (int i = 0; i < stop; i++) {
            final String field = keys.get(i);
            final Object newValue = data.get(field);
            Object oldValue = attributes.get(field);
            if (oldValue == null) {
                oldValue = "";
            }
            if (!newValue.equals(oldValue) && this.ticketFields.contains(field)) {
                modifiedKeys.add(field);
                final Change change = new Change(requestedId, currentTime, author, field, oldValue.toString(), newValue.toString());
                changeLog.add(change);
            }

        }
        if (!comment.trim().isEmpty()) {
            final String oldComment = Change.getLastComment(this.server.getDBConnection(), requestedId);
            TaskServer.LOGGER.finest("Last comment was : " + oldComment);
            if (!comment.equals(oldComment)) {
                final Change change = new Change(requestedId, currentTime, author, "comment", "", comment);
                changeLog.add(change);

            }
        }

        //
        if (!modifiedKeys.isEmpty()) {
            final StringBuilder builder = new StringBuilder();
            builder.append("UPDATE ticket SET changetime=?,");
            modifiedKeys.remove("changetime");
            final int modifiedKeysSize = modifiedKeys.size();
            for (int i = 0; i < modifiedKeysSize; i++) {
                final String field = modifiedKeys.get(i);
                builder.append(field);
                if (i < modifiedKeysSize - 1) {
                    builder.append("=?,");
                } else {
                    builder.append("=? ");
                }
            }
            builder.append(" WHERE id=" + requestedId);
            // Update ticket
            final PreparedStatement preparedStatement = this.server.getDBConnection().prepareStatement(builder.toString());
            preparedStatement.setLong(1, currentTime);
            for (int i = 0; i < modifiedKeysSize; i++) {
                final String field = modifiedKeys.get(i);
                final Object value = data.get(field);
                preparedStatement.setString(i + 2, value.toString());
            }
            preparedStatement.executeUpdate();
            preparedStatement.close();
        }

        // Log changes
        for (final Change change : changeLog) {
            change.commit(this.server.getDBConnection());
        }
        TaskServer.LOGGER.fine("Ticket " + requestedId + " updated by " + user + ".");
        //
        final List<Object> result = new ArrayList<>();
        result.add(requestedId);
        result.add(timeCreated);
        Date timeChanged = new Date(currentTime);
        result.add(timeChanged);
        result.add(attributes);
        return result;
    }

}