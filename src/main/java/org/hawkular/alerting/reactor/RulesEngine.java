package org.hawkular.alerting.reactor;

import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Simulates a rules engine running in background
 */
public class RulesEngine {
    private static final Logger log = LogManager.getLogger(RulesEngine.class);

    private static final String ENGINE_DELAY = "hawkular-alerts.engine-delay";
    private static final String ENGINE_PERIOD = "hawkular-alerts.engine-period";

    private Properties props;

    private final Timer wakeUpTimer;
    private TimerTask rulesTask;

    private int delay;
    private int period;


    public RulesEngine(Properties props) {
        this.props = props;
        delay = Integer.valueOf(props.getProperty(ENGINE_DELAY));
        period = Integer.valueOf(props.getProperty(ENGINE_PERIOD));
        wakeUpTimer = new Timer("RulesEngine-Timer");
    }

    public void start() {
        log.info("Starting RulesEngine [Period {} ms]", period);
        reload();
    }

    public void stop() {
        wakeUpTimer.cancel();
        wakeUpTimer.purge();
        log.info("Stopping RulesEngine");
    }

    public void reload() {
        rulesTask = new RulesInvoker();
        wakeUpTimer.schedule(rulesTask, delay, period);
    }

    private class RulesInvoker extends TimerTask {
        public void run() {
            log.info("Invoking Rules...");
        }
    }
}
