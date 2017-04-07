package org.hawkular.alerting.reactor;

/**
 * Administrative interface
 */
public interface AlertingServerMBean {
    String getStatus();
    void stop();
}
