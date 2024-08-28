package com.rawtask.sql;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.ListIterator;
import java.util.Properties;
import java.util.logging.Level;

import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;
import org.jdom2.output.Format;
import org.jdom2.output.XMLOutputter;

import com.rawtask.PasswordHash;
import com.rawtask.TaskServer;

public class DBTools {
    public static final int CHECK_CONNECTION_OK = 0;
    public static final int CHECK_CONNECTION_ERROR = 1;
    public static final int CHECK_CONNECTION_DB_NOTFOUND = 2;
    public static final String DB_PG_IP = "db.pg.ip";
    public static final String DB_PG_IP_DEFVALUE = "127.0.0.1";
    public static final String DB_PG_PORT = "db.pg.port";
    public static final String DB_PG_PORT_DEFVALUE = "5432";
    public static final String DB_PG_NAME = "db.pg.name";
    public static final String DB_PG_NAME_DEFVALUE = "rawtask";
    public static final String DB_PG_USER = "db.pg.user";
    public static final String DB_PG_USER_DEFVALUE = "postgres";
    public static final String DB_PG_PWD = "db.pg.pwd";
    public static final String DB_PG_PWD_DEFVALUE = "postgres";
    public static final String JDBC_PG = "jdbc:postgresql://";
    public static final String JDBC_H2 = "jdbc:h2:";
    public static final String DB_H2_FILENAME = "db.h2.filename";
    public static final String DB_H2_FILENAME_DEFVALUE = "rawtask";
    public static final String DB_H2_USER_DEFVALUE = "rawtask";
    public static final String DB_H2_PWD_DEFVALUE = "rawtask";
    public static final String DB_TYPE = "db.type";
    public static final String DB_TYPE_DEFVALUE = "H2";
    public static final String XML_ROOT_NAME = "rawtask";
    private Properties config;
    private Connection connection;

    public static void main(String[] args) throws Exception {
        // DBTools.CreateH2DBTables();
        // DBTools.CreatePgDB();
        // DBTools.CreatePgDBTables();
        // DBTools.H2ToXLM(new File("data", "backup"));
        // DBTools.XMLToPostgreSQL();

        DBTools dbTools = new DBTools(TaskServer.loadConfiguration("rawtask.properties"));
        dbTools.createPgDBTables();
        dbTools.importFromXML(new File("data/rawtask.xml"));
        // new
        // DBTools(TaskServer.loadConfiguration("rawtask.properties")).exportToXML(new
        // File("data/backup"));
    }

    public DBTools(Properties props) {
        this.config = props;
    }

    public boolean createPgDBTables() {
        try (Connection conn = this.createConnection(); Statement pgStatement = conn.createStatement();) {
            String pgSql = "CREATE TABLE \"rawtask_user\" (\"id\" bigserial, \"login\" text NOT NULL, \"password\" text NOT NULL,  CONSTRAINT \"user_pkey\" PRIMARY KEY (\"id\"))";

            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "INSERT INTO \"rawtask_user\" (login, password) VALUES ('admin', '" + PasswordHash.createHash("admin") + "')";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE TABLE \"component\" (\"id\" bigserial, \"name\" varchar(200) NOT NULL, \"owner\" text DEFAULT '',  \"description\" text DEFAULT '',  CONSTRAINT \"component_pkey\" PRIMARY KEY (\"id\"))";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "INSERT INTO \"component\" (\"name\", \"owner\") VALUES ('component1', 'noboby')";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "INSERT INTO \"component\" (\"name\", \"owner\") VALUES ('component2', 'noboby')";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE TABLE \"milestone\" (\"id\" bigserial, \"name\" varchar(200) NOT NULL, \"due\" bigint DEFAULT '0',  \"completed\" bigint DEFAULT '0',  \"description\" text DEFAULT '',  CONSTRAINT \"milestone_pkey\" PRIMARY KEY (\"id\"))";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "INSERT INTO \"milestone\" (\"name\") VALUES ('milestone1')";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "INSERT INTO \"milestone\" (\"name\") VALUES ('milestone2')";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE TABLE \"version\" (\"id\" bigserial, \"name\" varchar(200) NOT NULL,  \"time\" bigint DEFAULT '0', \"description\" text DEFAULT '',  CONSTRAINT \"version_pkey\" PRIMARY KEY (\"id\"))";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "INSERT INTO \"version\" (\"name\") VALUES ('1.0')";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "INSERT INTO \"version\" (\"name\") VALUES ('2.0')";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE TABLE \"ticket\" (\"id\" bigserial, \"type\" varchar(200), \"time\" bigint, \"changetime\" bigint, \"component\" varchar(200), \"severity\" varchar(200), \"priority\" varchar(200), \"owner\" varchar(200), \"reporter\" varchar(200), \"cc\" varchar(200), \"version\" varchar(200), \"milestone\" varchar(200), \"status\" varchar(200), \"resolution\" varchar(200), \"summary\" varchar(50000), \"description\" varchar(50000), \"keywords\" varchar(2000), CONSTRAINT \"ticket_pkey\" PRIMARY KEY (\"id\"))";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE INDEX \"ticket_status_idx\" ON \"ticket\"(\"status\")";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE INDEX \"ticket_time_idx\" ON \"ticket\"(\"time\")";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE TABLE \"ticket_change\" (\"ticket_id\" integer NOT NULL, \"time\" bigint NOT NULL, \"author\" varchar(200), \"field\" varchar(20) NOT NULL, \"oldvalue\" varchar(50000), \"newvalue\" varchar(50000), CONSTRAINT \"ticket_change_pk\" PRIMARY KEY (\"ticket_id\", \"time\", \"field\"))";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE INDEX \"ticket_change_ticket_idx\" ON \"ticket_change\"(\"ticket_id\")";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE INDEX \"ticket_change_time_idx\" ON \"ticket_change\"(\"time\")";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE TABLE \"attachment\" (\"ticket_id\" integer NOT NULL, \"filename\" varchar(250) NOT NULL, \"size\" integer, \"time\" bigint, \"description\" varchar(50000), \"author\" varchar(200), \"file\" varchar(250) NOT NULL, CONSTRAINT \"attachment_pk\" PRIMARY KEY (\"ticket_id\", \"filename\"))";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            return true;
        } catch (final SQLException e) {
            TaskServer.LOGGER.log(Level.SEVERE, "Erreur de connexion à la base " + e.getMessage());
            return false;
        } catch (final Exception e) {
            TaskServer.LOGGER.log(Level.SEVERE, "Database creation failed." + e.getMessage());
            return false;
        }
    }

    public Connection getConnection() throws SQLException {
        if (this.connection != null && this.connection.isClosed()) {
            this.connection = null;
        }
        if (this.connection == null) {
            this.connection = createConnection();
        }
        return this.connection;

    }

    private final Connection createConnection() throws SQLException {

        if (this.config.getProperty(DB_TYPE).equalsIgnoreCase("PostgreSQL")) {
            String stConnexion = JDBC_PG + this.config.getProperty(DB_PG_IP, DB_PG_IP_DEFVALUE) + ":" + this.config.getProperty(DB_PG_PORT, DB_PG_PORT_DEFVALUE) + "/"
                    + this.config.getProperty(DB_PG_NAME, DB_PG_NAME_DEFVALUE);
            TaskServer.LOGGER.log(Level.INFO, stConnexion);
            return DriverManager.getConnection(stConnexion, this.config.getProperty(DB_PG_USER, DB_PG_USER_DEFVALUE), this.config.getProperty(DB_PG_PWD, DB_PG_PWD_DEFVALUE));

        } else {
            try {
                File dataDir = new File("data");
                dataDir.mkdir();
                String stConnexion = JDBC_H2 + dataDir.getCanonicalFile() + "/" + this.config.getProperty(DB_H2_FILENAME, DB_H2_FILENAME_DEFVALUE) + ";MVCC=FALSE;MV_STORE=FALSE";

                return DriverManager.getConnection(stConnexion, DB_H2_USER_DEFVALUE, DB_H2_PWD_DEFVALUE);

            } catch (IOException e) {
                TaskServer.LOGGER.log(Level.SEVERE, "H2 database file access failed.", e);
                throw new SQLException("H2 database file access failed", e);
            }
        }

    }

    public int checkPgDBConnection() {
        int result = CHECK_CONNECTION_DB_NOTFOUND;
        try (Connection conn = this.createConnection();

                final Statement pgStatement = conn.createStatement();
                ResultSet pgRs = pgStatement.executeQuery("SELECT \"datname\" FROM \"pg_database\" WHERE \"datistemplate\" = false;")) {

            while (pgRs.next()) {
                if (pgRs.getString(1).equals(this.config.getProperty(DB_PG_NAME, DB_PG_NAME_DEFVALUE))) {
                    result = CHECK_CONNECTION_OK;
                }
            }

            return result;

        } catch (final Exception e) {
            TaskServer.LOGGER.severe("Cannot connect to database. " + e.getMessage());
            return CHECK_CONNECTION_ERROR;
        }
    }

    public boolean createPgDB() {
        try (Connection conn = DriverManager.getConnection(JDBC_PG + this.config.getProperty(DB_PG_IP, DB_PG_IP_DEFVALUE) + ":" + this.config.getProperty(DB_PG_PORT, DB_PG_PORT_DEFVALUE) + "/?",
                this.config.getProperty(DB_PG_USER, DB_PG_USER_DEFVALUE), this.config.getProperty(DB_PG_PWD, DB_PG_PWD_DEFVALUE));

                final Statement pgStatement = conn.createStatement();) {

            String pgSql = "CREATE DATABASE " + this.config.getProperty(DB_PG_NAME, DB_PG_NAME_DEFVALUE);
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);
            return true;

        } catch (final Exception e) {
            TaskServer.LOGGER.severe("Erreur lors de création de la base " + e.getMessage());
            return false;
        }
    }

    public boolean createH2DBTables() {

        final File dataDir = new File("data");
        if (!dataDir.exists()) {
            final boolean b = dataDir.mkdirs();
            if (!b) {
                TaskServer.LOGGER.severe("Unable to create data dir : " + dataDir.getAbsolutePath());
                return false;
            } else {
                TaskServer.LOGGER.info("Data directory : " + dataDir.getAbsolutePath() + " created.");
            }
        }

        try (Connection conn = DriverManager.getConnection(JDBC_H2 + dataDir.getCanonicalFile() + "/" + this.config.getProperty(DB_H2_FILENAME, DB_H2_FILENAME_DEFVALUE) + ";MVCC=FALSE;MV_STORE=FALSE",
                DB_H2_USER_DEFVALUE, DB_H2_PWD_DEFVALUE);

                final Statement pgStatement = conn.createStatement()) {

            String pgSql = "CREATE TABLE rawtask_user (id bigint auto_increment, login text NOT NULL, password text NOT NULL,  CONSTRAINT user_pkey PRIMARY KEY (id))";

            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "INSERT INTO rawtask_user (login, password) VALUES ('admin', '" + PasswordHash.createHash("admin") + "')";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE TABLE component (id bigint auto_increment, name varchar(200) NOT NULL, owner text DEFAULT '',  description text DEFAULT '',  CONSTRAINT component_pkey PRIMARY KEY (id))";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "INSERT INTO component(name, owner) VALUES ('component1', 'noboby')";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "INSERT INTO component (name, owner) VALUES ('component2', 'noboby')";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE TABLE milestone (id bigint auto_increment, name varchar(200) NOT NULL,  due bigint DEFAULT '0',  completed bigint DEFAULT '0',  description text DEFAULT '',  CONSTRAINT milestone_pkey PRIMARY KEY (id))";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "INSERT INTO milestone (name ) VALUES ('milestone1')";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "INSERT INTO milestone (name ) VALUES ('milestone2')";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE TABLE version (id bigint auto_increment, name varchar(200) NOT NULL,  time bigint DEFAULT '0',    description text DEFAULT '',  CONSTRAINT version_pkey PRIMARY KEY (id))";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "INSERT INTO version (name) VALUES ('1.0')";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "INSERT INTO version (name) VALUES ('2.0')";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE TABLE ticket (id bigint auto_increment, type varchar(200), time bigint, changetime bigint, component varchar(200), severity varchar(200), priority varchar(200), owner varchar(200), reporter varchar(200), cc varchar(200), version varchar(200), milestone varchar(200), status varchar(200), resolution varchar(200), summary varchar(50000), description varchar(50000), keywords varchar(2000), CONSTRAINT ticket_pkey PRIMARY KEY (id))";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE INDEX ticket_status_idx ON ticket(status)";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE INDEX ticket_time_idx ON ticket(time)";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE TABLE ticket_change (ticket_id integer NOT NULL, time bigint NOT NULL, author varchar(200), field varchar(20) NOT NULL, oldvalue varchar(50000), newvalue varchar(50000), CONSTRAINT ticket_change_pk PRIMARY KEY (ticket_id, time, field))";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE INDEX ticket_change_ticket_idx ON ticket_change(ticket_id)";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE INDEX ticket_change_time_idx ON ticket_change(time)";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            pgSql = "CREATE TABLE attachment (ticket_id integer NOT NULL, filename varchar(250) NOT NULL, size integer, time bigint, description varchar(50000), author varchar(200),file varchar(250) NOT NULL, CONSTRAINT attachment_pk PRIMARY KEY (ticket_id, filename))";
            TaskServer.LOGGER.finest(pgSql);
            pgStatement.execute(pgSql);

            return true;

        } catch (final Exception e) {
            TaskServer.LOGGER.severe("Erreur lors de création de la base " + e.getMessage());
            return false;
        }
    }

    public void exportToXML(File backupDir) throws SQLException, IOException {

        final File dataDir = new File("data");
        dataDir.mkdir();
        Connection conn = getConnection();
        try (final Statement stTickets = conn.createStatement();
                ResultSet pgrsTickets = stTickets.executeQuery("SELECT id, type, time, changetime, component, severity, priority, owner, reporter, cc, version, "
                        + "milestone, status, resolution, summary, description, keywords FROM ticket ORDER BY id");) {

            Element racine = new Element(XML_ROOT_NAME);
            Document document = new Document(racine);

            Element xmlTickets = new Element("tickets");

            while (pgrsTickets.next()) {
                Element ticket = new Element("ticket");

                if (pgrsTickets.getString(2) != null) {
                    ticket.setAttribute("type", pgrsTickets.getString(2));
                }
                if (pgrsTickets.getString(3) != null) {
                    ticket.setAttribute("time", String.valueOf(pgrsTickets.getLong(3)));
                }
                if (pgrsTickets.getString(4) != null) {
                    ticket.setAttribute("changetime", String.valueOf(pgrsTickets.getLong(4)));
                }
                if (pgrsTickets.getString(5) != null) {
                    ticket.setAttribute("component", pgrsTickets.getString(5));
                }
                if (pgrsTickets.getString(6) != null) {
                    ticket.setAttribute("severity", pgrsTickets.getString(6));
                }
                if (pgrsTickets.getString(7) != null) {
                    ticket.setAttribute("priority", pgrsTickets.getString(7));
                }
                if (pgrsTickets.getString(8) != null) {
                    ticket.setAttribute("owner", pgrsTickets.getString(8));
                }
                if (pgrsTickets.getString(9) != null) {
                    ticket.setAttribute("reporter", pgrsTickets.getString(9));
                }
                if (pgrsTickets.getString(10) != null) {
                    ticket.setAttribute("cc", pgrsTickets.getString(10));
                }
                if (pgrsTickets.getString(11) != null) {
                    ticket.setAttribute("version", pgrsTickets.getString(11));
                }
                if (pgrsTickets.getString(12) != null) {
                    ticket.setAttribute("milestone", pgrsTickets.getString(12));
                }
                if (pgrsTickets.getString(13) != null) {
                    ticket.setAttribute("status", pgrsTickets.getString(13));
                }
                if (pgrsTickets.getString(14) != null) {
                    ticket.setAttribute("resolution", pgrsTickets.getString(14));
                }
                if (pgrsTickets.getString(15) != null) {
                    ticket.setAttribute("summary", pgrsTickets.getString(15));
                }
                if (pgrsTickets.getString(16) != null) {
                    ticket.setAttribute("description", pgrsTickets.getString(16));
                }
                if (pgrsTickets.getString(17) != null) {
                    ticket.setAttribute("keywords", pgrsTickets.getString(17));
                }

                // Export ticket_change

                String sqlChanges = "SELECT ticket_id, time, author, field, oldvalue, newvalue FROM ticket_change WHERE ticket_id = " + pgrsTickets.getInt(1);
                try (Statement pgstChanges = conn.createStatement(); ResultSet pgrsChanges = pgstChanges.executeQuery(sqlChanges);) {

                    Element changes = new Element("changes");
                    ticket.addContent(changes);

                    while (pgrsChanges.next()) {
                        Element change = new Element("change");

                        if (pgrsChanges.getString(2) != null) {
                            change.setAttribute("time", String.valueOf(pgrsChanges.getLong(2)));
                        }
                        if (pgrsChanges.getString(3) != null) {
                            change.setAttribute("author", pgrsChanges.getString(3));
                        }
                        if (pgrsChanges.getString(4) != null) {
                            change.setAttribute("field", pgrsChanges.getString(4));
                        }
                        if (pgrsChanges.getString(5) != null) {
                            change.setAttribute("oldvalue", pgrsChanges.getString(5));
                        }
                        if (pgrsChanges.getString(6) != null) {
                            change.setAttribute("newvalue", pgrsChanges.getString(6));
                        }

                        changes.addContent(change);
                    }

                }

                // Export attachment.

                String sqlAttachments = "SELECT ticket_id, filename, size, time, description, author, file FROM attachment WHERE ticket_id = " + pgrsTickets.getInt(1);
                Statement stAttachments = conn.createStatement();
                ResultSet rsAttachments = stAttachments.executeQuery(sqlAttachments);

                Element attachments = new Element("attachments");
                ticket.addContent(attachments);

                while (rsAttachments.next()) {
                    Element attachment = new Element("attachment");

                    if (rsAttachments.getString(2) != null) {
                        attachment.setAttribute("filename", rsAttachments.getString(2));
                    }
                    if (rsAttachments.getString(3) != null) {
                        attachment.setAttribute("size", String.valueOf(rsAttachments.getInt(3)));
                    }
                    if (rsAttachments.getString(4) != null) {
                        attachment.setAttribute("time", String.valueOf(rsAttachments.getLong(4)));
                    }
                    if (rsAttachments.getString(5) != null) {
                        attachment.setAttribute("description", rsAttachments.getString(5));
                    }
                    if (rsAttachments.getString(6) != null) {
                        attachment.setAttribute("author", rsAttachments.getString(6));
                    }
                    if (rsAttachments.getString(7) != null) {
                        attachment.setAttribute("file", rsAttachments.getString(7));
                    }

                    attachments.addContent(attachment);
                }

                xmlTickets.addContent(ticket);
            }

            racine.addContent(xmlTickets);

            TaskServer.LOGGER.log(Level.INFO, "Export tickets, ticket_changes, attachements to xml document.");

            // Export rawtask_user.

            try (Statement pgstUsers = conn.createStatement(); ResultSet pgrsUsers = pgstUsers.executeQuery("SELECT id, login, password FROM rawtask_user");) {

                Element users = new Element("users");

                while (pgrsUsers.next()) {
                    Element user = new Element("user");

                    if (pgrsUsers.getString(2) != null) {
                        user.setAttribute("login", pgrsUsers.getString(2));
                    }
                    if (pgrsUsers.getString(3) != null) {
                        user.setAttribute("password", pgrsUsers.getString(3));
                    }

                    users.addContent(user);
                }
                racine.addContent(users);
            }
            TaskServer.LOGGER.log(Level.INFO, "Export users to xml document.");

            // Export component.

            try (Statement stComponents = conn.createStatement(); ResultSet rsComponents = stComponents.executeQuery("SELECT name, owner, description FROM component");) {

                Element components = new Element("components");

                while (rsComponents.next()) {
                    Element component = new Element("component");

                    if (rsComponents.getString(1) != null) {
                        component.setAttribute("name", rsComponents.getString(1));
                    }
                    if (rsComponents.getString(2) != null) {
                        component.setAttribute("owner", rsComponents.getString(2));
                    }
                    if (rsComponents.getString(3) != null) {
                        component.setAttribute("description", rsComponents.getString(3));
                    }

                    components.addContent(component);
                }

                racine.addContent(components);
            }
            TaskServer.LOGGER.log(Level.INFO, "Export components to xml document.");

            // Export milestone.

            try (Statement stMilestones = conn.createStatement(); ResultSet rsMilestones = stMilestones.executeQuery("SELECT name, due, completed, description FROM milestone");)

            {

                Element milestones = new Element("milestones");

                while (rsMilestones.next()) {
                    Element milestone = new Element("milestone");

                    if (rsMilestones.getString(1) != null) {
                        milestone.setAttribute("name", rsMilestones.getString(1));
                    }
                    if (rsMilestones.getString(2) != null) {
                        milestone.setAttribute("due", String.valueOf(rsMilestones.getLong(2)));
                    }
                    if (rsMilestones.getString(3) != null) {
                        milestone.setAttribute("completed", String.valueOf(rsMilestones.getLong(3)));
                    }
                    if (rsMilestones.getString(4) != null) {
                        milestone.setAttribute("description", rsMilestones.getString(4));
                    }

                    milestones.addContent(milestone);
                }

                racine.addContent(milestones);
            }
            TaskServer.LOGGER.log(Level.INFO, "Export milestones to xml document.");

            // Export version.

            try (Statement stVersions = conn.createStatement(); ResultSet rsVersions = stVersions.executeQuery("SELECT name, time, description FROM version");) {

                Element versions = new Element("versions");

                while (rsVersions.next()) {
                    Element version = new Element("version");

                    if (rsVersions.getString(1) != null) {
                        version.setAttribute("name", rsVersions.getString(1));
                    }
                    if (rsVersions.getString(2) != null) {
                        version.setAttribute("time", String.valueOf(rsVersions.getLong(2)));
                    }
                    if (rsVersions.getString(3) != null) {
                        version.setAttribute("description", rsVersions.getString(3));
                    }

                    versions.addContent(version);
                }

                racine.addContent(versions);
            }
            TaskServer.LOGGER.log(Level.INFO, "Export versions to xml document.");

            File f = new File(backupDir, this.config.getProperty("xml.dump.filename", "rawtask.xml"));

            XMLOutputter sortie = new XMLOutputter(Format.getPrettyFormat());
            sortie.output(document, new FileWriter(f));

            TaskServer.LOGGER.log(Level.INFO, "Backup successful :  (" + f.length() / 1024 + " kilobytes)");

        }
    }

    /**
     * Import from an XML file
     * 
     * @param backupFile XML file from a previous backup with exportToXML
     * @throws SQLException
     * @see exportToXML
     */
    public boolean importFromXML(File backupFile) throws SQLException, IOException, JDOMException {
        Connection conn = getConnection();

        SAXBuilder builder = new SAXBuilder();
        File xmlFile = backupFile;
        if (!xmlFile.exists()) {
            throw new IllegalArgumentException("file " + xmlFile.getAbsolutePath() + " not found");
        }
        Document document = builder.build(xmlFile);
        try {
            // Database cleanup.

            Statement stCleaner = conn.createStatement();
            stCleaner.addBatch("DELETE FROM ticket_change");
            stCleaner.addBatch("DELETE FROM attachment");
            stCleaner.addBatch("DELETE FROM ticket");
            stCleaner.addBatch("DELETE FROM component");
            stCleaner.addBatch("DELETE FROM milestone");
            stCleaner.addBatch("DELETE FROM version");
            stCleaner.addBatch("DELETE FROM rawtask_user");

            if (this.config.getProperty(DB_TYPE, DB_TYPE_DEFVALUE).equalsIgnoreCase("PostgreSQL")) {
                stCleaner.addBatch("ALTER SEQUENCE ticket_id_seq RESTART");
            } else {
                stCleaner.addBatch("ALTER TABLE ticket ALTER COLUMN id RESTART WITH 1");
            }

            stCleaner.executeBatch();
            stCleaner.close();

            Element racine = document.getRootElement();

            // Import ticket.

            Element elmTickets = racine.getChild("tickets");

            List<Element> lstTicket = elmTickets.getChildren("ticket");
            ListIterator<Element> itrTicket = lstTicket.listIterator();
            TaskServer.LOGGER.log(Level.INFO, "Database erased");

            while (itrTicket.hasNext()) {
                Element elmTicket = itrTicket.next();
                System.out.println(elmTicket.toString());

                PreparedStatement pgst_ticket = conn.prepareStatement("INSERT INTO ticket (type, time, changetime, component, severity, priority"
                        + ", owner, reporter, cc, version, milestone, status, resolution, summary, description" + ", keywords ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        Statement.RETURN_GENERATED_KEYS);

                pgst_ticket.setString(1, elmTicket.getAttributeValue("type"));
                if (elmTicket.getAttributeValue("time") != null) {
                    pgst_ticket.setLong(2, Long.valueOf(elmTicket.getAttributeValue("time")));
                } else {
                    pgst_ticket.setNull(2, Types.BIGINT);
                }
                if (elmTicket.getAttributeValue("changetime") != null) {
                    pgst_ticket.setLong(3, Long.valueOf(elmTicket.getAttributeValue("changetime")));
                } else {
                    pgst_ticket.setNull(3, Types.BIGINT);
                }
                pgst_ticket.setString(4, elmTicket.getAttributeValue("component"));
                pgst_ticket.setString(5, elmTicket.getAttributeValue("severity"));
                pgst_ticket.setString(6, elmTicket.getAttributeValue("priority"));
                pgst_ticket.setString(7, elmTicket.getAttributeValue("owner"));
                pgst_ticket.setString(8, elmTicket.getAttributeValue("reporter"));
                pgst_ticket.setString(9, elmTicket.getAttributeValue("cc"));
                pgst_ticket.setString(10, elmTicket.getAttributeValue("version"));
                pgst_ticket.setString(11, elmTicket.getAttributeValue("milestone"));
                pgst_ticket.setString(12, elmTicket.getAttributeValue("status"));
                pgst_ticket.setString(13, elmTicket.getAttributeValue("resolution"));
                pgst_ticket.setString(14, elmTicket.getAttributeValue("summary"));
                pgst_ticket.setString(15, elmTicket.getAttributeValue("description"));
                pgst_ticket.setString(16, elmTicket.getAttributeValue("keywords"));

                pgst_ticket.executeUpdate();

                ResultSet rsTicket = pgst_ticket.getGeneratedKeys();
                long keyTicket = 0;
                if (rsTicket.next()) {
                    keyTicket = rsTicket.getInt(1);
                }

                Element elmChanges = elmTicket.getChild("changes");

                List<Element> lstChange = elmChanges.getChildren("change");
                ListIterator<Element> itrChange = lstChange.listIterator();

                while (itrChange.hasNext()) {
                    Element elmChange = (Element) itrChange.next();

                    PreparedStatement stChange = conn.prepareStatement("INSERT INTO ticket_change (ticket_id, time, author, field, oldvalue, newvalue) VALUES (?,?,?,?,?,?)");
                    stChange.setLong(1, keyTicket);

                    if (elmChange.getAttributeValue("time") != null) {
                        stChange.setLong(2, Long.valueOf(elmChange.getAttributeValue("time")));
                    } else {
                        throw new IllegalStateException("no attribute 'time'");
                    }
                    stChange.setString(3, elmChange.getAttributeValue("author"));
                    if (elmChange.getAttributeValue("field") != null) {
                        stChange.setString(4, String.valueOf(elmChange.getAttributeValue("field")));
                    } else {
                        throw new IllegalStateException("no attribute 'field'");
                    }
                    stChange.setString(5, elmChange.getAttributeValue("oldvalue"));
                    stChange.setString(6, elmChange.getAttributeValue("newvalue"));

                    stChange.executeUpdate();

                }

                Element elmAttachments = elmTicket.getChild("attachments");

                List<Element> lstAttachment = elmAttachments.getChildren("attachment");
                ListIterator<Element> itrAttachment = lstAttachment.listIterator();

                while (itrAttachment.hasNext()) {
                    Element elmAttachment = (Element) itrAttachment.next();
                    System.out.println(elmAttachment.toString());

                    PreparedStatement pgst_attachment = conn.prepareStatement(" INSERT INTO attachment (ticket_id, filename, size, time, description, author, file) VALUES (?,?,?,?,?,?,?)");
                    pgst_attachment.setLong(1, keyTicket);
                    if (elmAttachment.getAttributeValue("filename") != null) {
                        pgst_attachment.setString(2, elmAttachment.getAttributeValue("filename"));
                    } else {
                        throw new IllegalStateException("no attribute 'filename'");
                    }
                    if (elmAttachment.getAttributeValue("size") != null) {
                        pgst_attachment.setInt(3, Integer.valueOf(elmAttachment.getAttributeValue("size")));
                    } else {
                        pgst_attachment.setNull(3, Types.INTEGER);
                    }
                    if (elmAttachment.getAttributeValue("time") != null) {
                        pgst_attachment.setLong(4, Long.valueOf(elmAttachment.getAttributeValue("time")));
                    } else {
                        pgst_attachment.setNull(4, Types.BIGINT);
                    }
                    pgst_attachment.setString(5, elmAttachment.getAttributeValue("description"));
                    pgst_attachment.setString(6, elmAttachment.getAttributeValue("author"));
                    if (elmAttachment.getAttributeValue("file") != null) {
                        pgst_attachment.setString(7, elmAttachment.getAttributeValue("file"));
                    } else {
                        throw new IllegalStateException("no attribute 'file'");
                    }

                    pgst_attachment.executeUpdate();
                }
            }
            TaskServer.LOGGER.log(Level.INFO, "Tickets imported with changes and attachement.");

            // Import user.

            Element elmUsers = racine.getChild("users");

            List<Element> lstUser = elmUsers.getChildren("user");
            ListIterator<Element> itrUser = lstUser.listIterator();

            try (PreparedStatement stUser = conn.prepareStatement("INSERT INTO rawtask_user (login, password) VALUES (?,?)");) {
                while (itrUser.hasNext()) {
                    Element elmUser = itrUser.next();

                    if (elmUser.getAttributeValue("login") != null) {
                        stUser.setString(1, elmUser.getAttributeValue("login"));
                    } else {
                        throw new IllegalStateException("no attribute 'login'");
                    }
                    if (elmUser.getAttributeValue("password") != null) {
                        stUser.setString(2, elmUser.getAttributeValue("password"));
                    } else {
                        throw new IllegalStateException("no attribute 'password'");
                    }
                    stUser.executeUpdate();
                }
            }
            TaskServer.LOGGER.log(Level.INFO, "Users imported.");

            // Import component.

            Element elmComponents = racine.getChild("components");

            List<Element> lstComponent = elmComponents.getChildren("component");
            ListIterator<Element> itrComponent = lstComponent.listIterator();

            try (PreparedStatement stComponent = conn.prepareStatement("INSERT INTO component (name, owner, description) VALUES (?,?,?)");) {
                while (itrComponent.hasNext()) {
                    Element elmComponent = itrComponent.next();

                    if (elmComponent.getAttributeValue("name") != null) {
                        stComponent.setString(1, elmComponent.getAttributeValue("name"));
                    } else {
                        throw new IllegalStateException("no attribute 'name'");
                    }
                    stComponent.setString(2, String.valueOf(elmComponent.getAttributeValue("owner")));
                    stComponent.setString(3, String.valueOf(elmComponent.getAttributeValue("description")));

                    stComponent.executeUpdate();
                }
            }
            TaskServer.LOGGER.log(Level.INFO, "Components imported.");

            // Import milestone

            Element elmMilestones = racine.getChild("milestones");

            List<Element> lstMilestone = elmMilestones.getChildren("milestone");
            ListIterator<Element> itrMilestone = lstMilestone.listIterator();

            try (PreparedStatement stMilestone = conn.prepareStatement("INSERT INTO milestone (name, due, completed, description) VALUES (?,?,?,?)");) {
                while (itrMilestone.hasNext()) {
                    Element elmMilestone = itrMilestone.next();

                    if (elmMilestone.getAttributeValue("name") != null) {
                        stMilestone.setString(1, String.valueOf(elmMilestone.getAttributeValue("name")));
                    } else {
                        throw new IllegalStateException("no attribute 'name'");
                    }
                    if (elmMilestone.getAttributeValue("due") != null) {
                        stMilestone.setLong(2, Long.valueOf(elmMilestone.getAttributeValue("due")));
                    } else {
                        stMilestone.setNull(2, Types.BIGINT);
                    }

                    if (elmMilestone.getAttributeValue("completed") != null) {
                        stMilestone.setLong(3, Long.valueOf(elmMilestone.getAttributeValue("completed")));
                    } else {
                        stMilestone.setNull(3, Types.BIGINT);
                    }
                    stMilestone.setString(4, elmMilestone.getAttributeValue("description"));

                    stMilestone.executeUpdate();
                }
            }
            TaskServer.LOGGER.log(Level.INFO, "Milestones imported.");

            // Import version

            Element elmVersions = racine.getChild("versions");

            List<Element> lstVersion = elmVersions.getChildren("version");
            ListIterator<Element> itrVersion = lstVersion.listIterator();

            try (PreparedStatement stVersion = conn.prepareStatement("INSERT INTO version (name, time, description) VALUES (?,?,?)");) {
                while (itrVersion.hasNext()) {
                    Element elmVersion = itrVersion.next();

                    if (elmVersion.getAttributeValue("name") != null) {
                        stVersion.setString(1, String.valueOf(elmVersion.getAttributeValue("name")));
                    } else {
                        throw new IllegalStateException("no attribute 'name'");
                    }
                    if (elmVersion.getAttributeValue("time") != null) {
                        stVersion.setLong(2, Long.valueOf(elmVersion.getAttributeValue("time")));
                    } else {
                        stVersion.setNull(2, Types.BIGINT);
                    }
                    if (elmVersion.getAttributeValue("description") != null) {
                        stVersion.setString(3, String.valueOf(elmVersion.getAttributeValue("description")));
                    } else {
                        stVersion.setNull(3, Types.BIGINT);
                    }
                    stVersion.executeUpdate();
                }
            }
            TaskServer.LOGGER.log(Level.INFO, "Versions imported.");

            return true;
        } catch (final SQLException e) {
            TaskServer.LOGGER.log(Level.SEVERE, "SQL error on import", e);
            return false;
        }
    }
}
