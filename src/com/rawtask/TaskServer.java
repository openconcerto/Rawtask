package com.rawtask;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.jetty.http.HttpVersion;
import org.eclipse.jetty.security.ConstraintMapping;
import org.eclipse.jetty.security.ConstraintSecurityHandler;
import org.eclipse.jetty.security.authentication.BasicAuthenticator;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlets.gzip.GzipHandler;
import org.eclipse.jetty.util.security.Constraint;
import org.eclipse.jetty.util.ssl.SslContextFactory;

import com.rawtask.sql.BackupScheduler;
import com.rawtask.sql.DBTools;
import com.rawtask.www.AdminHandler;
import com.rawtask.www.AdminPostHandler;
import com.rawtask.www.LogoutHandler;
import com.rawtask.www.PublicResourceHandler;
import com.rawtask.www.UserAdminHandler;
import com.rawtask.xmlrpc.XMLRpcHandler;

public class TaskServer {
    public static final String RAWTASK_PROPERTIES = "rawtask.properties";

    private static final String KEYSTORE_JKS = "keystore.jks";

    public static final Logger LOGGER = Logger.getLogger("com.rawtask.TaskServer");
    private DBTools dbTools;

    public static void main(String[] args) throws Exception {
        Class.forName("org.h2.Driver");
        Class.forName("org.postgresql.Driver");

        Options options = new Options();

        Option input = new Option("i", "import", true, "xml to postgresql import");
        input.setRequired(false);
        options.addOption(input);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("i")) {
                System.out.println("Import" + cmd.getOptionValue("i"));

                File backupDir = new File(cmd.getOptionValue("i"));
                DBTools dbTools = new DBTools(loadConfiguration(RAWTASK_PROPERTIES));
                boolean res = dbTools.importFromXML(backupDir);
                dbTools.getConnection().close();
                if (res) {
                    TaskServer.LOGGER.info("Backup file successfuly imported to Postgresql.");
                } else {
                    TaskServer.LOGGER.info("Import failed.");
                }

                System.exit(0);
            }

        } catch (ParseException e) {
            formatter.printHelp("utility-name", options);
            TaskServer.LOGGER.log(Level.SEVERE, "Bad argument : Exiting.", e);
            System.exit(1);
        } catch (NullPointerException e) {
            TaskServer.LOGGER.log(Level.SEVERE, "Import file error : Exiting.", e);
            System.exit(1);
        }

        final Logger rootLogger = Logger.getLogger("");

        for (final Handler h : rootLogger.getHandlers()) {
            rootLogger.removeHandler(h);
        }
        final ConsoleHandler handler = new ConsoleHandler();
        handler.setLevel(Level.FINEST);
        handler.setFormatter(new SimpleLogFormatter());
        TaskServer.LOGGER.setLevel(Level.FINEST);
        TaskServer.LOGGER.addHandler(handler);
        final TaskServer server = new TaskServer();

        if (server.init()) {
            server.start();
        } else {
            TaskServer.LOGGER.severe("Initialization failed. Exiting.");
        }
    }

    private final Properties properties;

    private BackupScheduler backup;

    private Connection conn;

    TaskServer() {
        this.properties = loadConfiguration(RAWTASK_PROPERTIES);

    }

    public static String readUTF8String(final InputStream is, final int bufferSize) {
        final char[] buffer = new char[bufferSize];
        final StringBuilder out = new StringBuilder();
        try (Reader in = new InputStreamReader(is, "UTF-8")) {
            for (;;) {
                final int rsz = in.read(buffer, 0, buffer.length);
                if (rsz < 0) {
                    break;
                }
                out.append(buffer, 0, rsz);
            }
        } catch (final Exception ex) {
            ex.printStackTrace();
        }
        return out.toString();
    }

    public File getDataDir() {
        return new File("data");
    }

    private boolean init() throws IOException, SQLException {
        File ks = new File(KEYSTORE_JKS);
        if (!ks.exists()) {
            TaskServer.LOGGER.severe("Keystore file missing : " + ks.getCanonicalPath());
            return false;
        } else if (!ks.canRead()) {
            TaskServer.LOGGER.severe("Cannot read keystore file  : " + ks.getCanonicalPath());
            return false;
        }
        // create folders if needed
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
        if (dataDir.isDirectory() && dataDir.canRead() && dataDir.canWrite()) {
            final File attaDir = new File(dataDir, "attachments");
            if (!attaDir.exists()) {
                final boolean b = attaDir.mkdirs();
                if (!b) {
                    TaskServer.LOGGER.severe("Unable to create attachments dir : " + attaDir.getAbsolutePath());
                    return false;
                } else {
                    TaskServer.LOGGER.info("Attachments directory : " + attaDir.getAbsolutePath() + " created.");
                }

            }
            if (!attaDir.isDirectory() || !attaDir.canRead() || !attaDir.canWrite()) {
                TaskServer.LOGGER.severe("cannot access directory : " + attaDir.getAbsolutePath());
            }
        }

        dbTools = new DBTools(this.properties);

        if (properties.getProperty("db.type", "H2").equals("H2")) {
            final File file = new File(dataDir, this.properties.getProperty("db.h2.filename", "rawtask") + ".h2.db");

            if (!file.exists()) {
                TaskServer.LOGGER.info("No database found, creating H2 database.");

                if (!dbTools.createH2DBTables()) {
                    return false;
                }
            }

        } else if (properties.getProperty("db.type", "H2").equals("PostgreSQL")) {
            // Check if database exist and if not, create a new one.
            if ((dbTools.checkPgDBConnection()) == DBTools.CHECK_CONNECTION_DB_NOTFOUND) {
                if (dbTools.createPgDB()) {
                    if (!dbTools.createPgDBTables()) {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }

        final File backupDir = new File(dataDir, "backup");
        if (!backupDir.exists()) {
            final boolean b = backupDir.mkdirs();
            if (!b) {
                TaskServer.LOGGER.severe("Unable to create data dir : " + backupDir.getAbsolutePath());
                return false;
            } else {
                TaskServer.LOGGER.info("Backup directory : " + backupDir.getAbsolutePath() + " created.");
            }
        }

        backup = new BackupScheduler(backupDir, dbTools);

        return true;

    }

    private void start() throws Exception {

        final int port = Integer.parseInt(this.properties.getProperty("srv.port", "8443"));

        // Shutdown by closing connection
        final Thread shutdownHook = new Thread() {
            @Override
            public void run() {
                try {
                    conn.close();
                    TaskServer.LOGGER.info("Rawtask server stopped.");
                } catch (final SQLException e) {
                    TaskServer.LOGGER.info("Rawtask server stopped but database not closed.");
                }
            }
        };

        if (Integer.valueOf(this.properties.getProperty("db.h2.backup.enabled", "1")) == 1 && this.properties.getProperty("db.type").equals("H2")) {
            backup.start(Integer.valueOf(this.properties.getProperty("db.h2.backup.hour", "22")));
        }

        Runtime.getRuntime().addShutdownHook(shutdownHook);

        // Start web server
        final HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setSecureScheme("https");
        httpConfig.setSecurePort(port);
        httpConfig.setOutputBufferSize(32768);
        httpConfig.setRequestHeaderSize(8192);
        httpConfig.setResponseHeaderSize(8192);
        httpConfig.setSendServerVersion(true);
        httpConfig.setSendDateHeader(false);

        final Server server = new Server();
        final SslContextFactory sslContextFactory = new SslContextFactory();
        sslContextFactory.setKeyStorePath(KEYSTORE_JKS);
        sslContextFactory.setKeyStorePassword("rawtask");
        sslContextFactory.setKeyManagerPassword("rawtask");

        sslContextFactory.setCertAlias("rawtask");
        sslContextFactory.setTrustStorePath(KEYSTORE_JKS);
        sslContextFactory.setTrustStorePassword("rawtask");
        sslContextFactory.setExcludeCipherSuites("SSL_RSA_WITH_DES_CBC_SHA", "SSL_DHE_RSA_WITH_DES_CBC_SHA", "SSL_DHE_DSS_WITH_DES_CBC_SHA", "SSL_RSA_EXPORT_WITH_RC4_40_MD5",
                "SSL_RSA_EXPORT_WITH_DES40_CBC_SHA", "SSL_DHE_RSA_EXPORT_WITH_DES40_CBC_SHA", "SSL_DHE_DSS_EXPORT_WITH_DES40_CBC_SHA");

        // SSL HTTP Configuration
        final HttpConfiguration httpsConfig = new HttpConfiguration(httpConfig);
        httpsConfig.addCustomizer(new SecureRequestCustomizer());

        // SSL Connector
        final ServerConnector sslConnector = new ServerConnector(server, new SslConnectionFactory(sslContextFactory, HttpVersion.HTTP_1_1.asString()), new HttpConnectionFactory(httpsConfig));
        sslConnector.setPort(8443);
        server.addConnector(sslConnector);
        // Security

        final GloablLoginService loginService = new GloablLoginService(this, this.properties.getProperty("srv.admin.password", "admin"));

        server.addBean(loginService);
        final ConstraintSecurityHandler security = new ConstraintSecurityHandler();
        server.setHandler(security);
        final Constraint constraint = new Constraint();
        constraint.setName("auth");
        constraint.setAuthenticate(true);
        constraint.setRoles(new String[] { "user", "admin" });
        final ConstraintMapping mapping = new ConstraintMapping();
        mapping.setPathSpec("/*");
        mapping.setConstraint(constraint);

        final ConstraintMapping mapping2 = new ConstraintMapping();
        mapping2.setPathSpec("/rawtask/login/xmlrpc");
        mapping2.setConstraint(constraint);

        final ConstraintMapping mapping3 = new ConstraintMapping();
        mapping3.setPathSpec("/rawtask/login");
        mapping3.setConstraint(constraint);

        final List<ConstraintMapping> mappings = new ArrayList<>(3);
        mappings.add(mapping);
        mappings.add(mapping2);
        mappings.add(mapping3);
        security.setConstraintMappings(mappings);

        security.setAuthenticator(new BasicAuthenticator());

        security.setLoginService(loginService);
        final HandlerList list = new HandlerList();
        final GzipHandler gzip = new GzipHandler();
        gzip.addIncludedMethods("GET", "POST");
        gzip.setMinGzipSize(0);
        gzip.addIncludedMimeTypes("text/xml", "application/xml", "text/plain", "text/css", "text/html", "application/javascript");
        gzip.addIncludedPaths("/*");

        final PublicResourceHandler publicResourceHandler = new PublicResourceHandler(new File("resources"));
        list.addHandler(new LogoutHandler());
        list.addHandler(publicResourceHandler);
        list.addHandler(new UserAdminHandler(this));
        list.addHandler(new AdminHandler(this));

        list.addHandler(new AdminPostHandler(this, loginService));
        list.addHandler(new XMLRpcHandler(this));

        security.setHandler(list);
        //
        final HandlerList h = new HandlerList();
        h.addHandler(security);
        gzip.setHandler(h);
        server.setHandler(gzip);

        server.start();
        final Enumeration<NetworkInterface> e = NetworkInterface.getNetworkInterfaces();
        while (e.hasMoreElements()) {
            final NetworkInterface n = e.nextElement();
            final Enumeration<InetAddress> ee = n.getInetAddresses();
            while (ee.hasMoreElements()) {
                final InetAddress i = ee.nextElement();
                final String hostAddress = i.getHostAddress();
                if (!hostAddress.contains(":")) {
                    if (port != 443) {
                        TaskServer.LOGGER.info("Rawtask server ready and listening on : https://" + hostAddress + ":" + port + "/rawtask");
                    } else {
                        TaskServer.LOGGER.info("Rawtask server ready and listening on : https://" + hostAddress + "/rawtask");

                    }
                }
            }
        }
        server.join();
    }

    public static Properties loadConfiguration(String configFileName) {
        Properties config = new Properties();

        final File configFile = new File(configFileName);

        try {
            final FileInputStream inStream = new FileInputStream(configFile);
            config.load(inStream);
            inStream.close();
            TaskServer.LOGGER.info("Settings loaded from " + configFile.getCanonicalPath());
            TaskServer.LOGGER.info("Using " + config.getProperty("db.type"));
        } catch (final IOException e) {
            TaskServer.LOGGER.severe("Unable to read config file : " + configFile.getAbsolutePath());
            TaskServer.LOGGER.info("Using default settings");
        }

        return config;
    }

    public synchronized Connection getDBConnection() throws SQLException {
        return this.dbTools.getConnection();
    }

}
