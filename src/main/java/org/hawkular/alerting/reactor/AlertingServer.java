package org.hawkular.alerting.reactor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.Properties;

import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.http.server.HttpServer;

/**
 * Main server class.
 */
public class AlertingServer implements AlertingServerMBean {
    private static final Logger log = LogManager.getLogger(AlertingServer.class);
    private static final String PROPS_FILE = "alerting.properties";

    private Properties props;

    private NettyContext context;

    private RestHandlers rest;
    private RulesEngine rules;
    private PartitionManager partition;

    public void setup() {
        log.info("Loading properties");
        String pathFile = System.getProperty(PROPS_FILE);
        InputStream is = null;
        if (pathFile != null) {
            File file = new File(pathFile);
            if (file.exists()) {
                try {
                    is = new FileInputStream(file);
                } catch (FileNotFoundException e) {
                    log.warn("Properties file {} not found", pathFile);
                }
            }
        }
        if (is == null) {
            is = AlertingServer.class.getClassLoader().getResourceAsStream(PROPS_FILE);
        }
        props = new Properties();
        try {
            props.load(is);
        } catch (IOException e) {
            log.fatal("Properties file cannot be loaded.");
            System.exit(1);
        }
    }

    public void start() {
        String bindAdress = props.getProperty("bind-address");
        Integer port = Integer.valueOf(props.getProperty("port"));
        log.info("Starting Server at http://{}:{}", bindAdress, port);
        rules = new RulesEngine(props);
        rules.start();
        partition = new PartitionManager(props);
        partition.start();
        rest = new RestHandlers(props);
        context = HttpServer.create(bindAdress, port)
                .newRouter(r -> r
                        .route(req -> true, (req, resp) -> rest.process(req, resp)))
                .block();
        context.onClose().block();
    }

    public String getStatus() {
        return context != null ? "STARTED" : "STOPPED";
    }

    public void stop() {
        partition.stop();
        rules.stop();
        context.dispose();
        log.info("Stopping Server");
        System.exit(0);
    }

    public static void registerMBean(AlertingServer server) {
        try {
            ObjectName jmxName = new ObjectName("org.hawkular.alerting:name=AlertingServer");
            ManagementFactory.getPlatformMBeanServer().registerMBean(server, jmxName);
        } catch (Exception exception) {
            log.error("Unable to register JMX Bean");
        }
    }

    public static void main(String[] args) {
        AlertingServer server = new AlertingServer();
        registerMBean(server);
        server.setup();
        server.start();
    }
}
