package org.hawkular.alerts.netty;

import java.lang.management.ManagementFactory;

import javax.management.ObjectName;

import org.hawkular.alerts.actions.standalone.StandaloneActionPluginRegister;
import org.hawkular.alerts.engine.StandaloneAlerts;
import org.hawkular.alerts.log.MsgLogger;
import org.hawkular.alerts.properties.AlertProperties;
import org.jboss.logging.Logger;

import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.http.server.HttpServer;

/**
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
public class AlertingServer implements AlertingServerMBean {
    private static final MsgLogger log = Logger.getMessageLogger(MsgLogger.class, AlertingServer.class.getName());

    private static final String BIND_ADDRESS = "hawkular-alerts.bind-address";
    private static final String BIND_ADDRESS_DEFAULT = "127.0.0.1";
    private static final String PORT = "hawkular-alerts.port";
    private static final String PORT_DEFAULT = "8080";
    private static final String JMX_NAME = "org.hawkular.alerting:name=AlertingServer";

    private NettyContext context;
    private HandlersManager handlers;

    public void start() {
        String bindAdress = AlertProperties.getProperty(BIND_ADDRESS, BIND_ADDRESS_DEFAULT);
        Integer port = Integer.valueOf(AlertProperties.getProperty(PORT, PORT_DEFAULT));

        log.infof("Starting Server at http://%s:%s", bindAdress, port);

        StandaloneAlerts.start();
        handlers = new HandlersManager();
        handlers.start();
        StandaloneActionPluginRegister.start();
        try {
            context = HttpServer.create(bindAdress, port)
                    .newRouter(r -> r
                            .route(req -> true, (req, resp) -> handlers.process(req, resp)))
                    .block();
        } catch (Exception e) {
            log.fatal(e);
            log.fatal("Forcing exit");
            StandaloneActionPluginRegister.stop();
            StandaloneAlerts.stop();
            System.exit(1);
        }
        context.onClose().block();
    }

    public String getStatus() {
        return context != null ? "STARTED" : "STOPPED";
    }

    public void stop() {
        log.info("Stopping Server");
        StandaloneActionPluginRegister.stop();
        StandaloneAlerts.stop();
        log.info("Server stopped");
        System.exit(0);
    }

    public static void registerMBean(AlertingServer server) {
        try {
            ObjectName jmxName = new ObjectName(JMX_NAME);
            ManagementFactory.getPlatformMBeanServer().registerMBean(server, jmxName);
        } catch (Exception exception) {
            log.error("Unable to register JMX Bean");
        }
    }

    public static void main(String[] args) {
        AlertingServer server = new AlertingServer();
        registerMBean(server);
        server.start();
    }
}
