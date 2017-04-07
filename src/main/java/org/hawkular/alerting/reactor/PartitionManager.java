package org.hawkular.alerting.reactor;

import java.io.IOException;
import java.util.Properties;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;

/**
 * It manages all the scalability services
 */
public class PartitionManager {
    private static final Logger log = LogManager.getLogger(PartitionManager.class);
    private static final String ISPN_CONFIG = "/alerting-ispn.xml";

    private Properties props;

    private EmbeddedCacheManager cacheManager;

    public PartitionManager(Properties props) {
        this.props = props;
    }

    public void start() {
        log.info("Starting PartitionManager");
        try {
            cacheManager = new DefaultCacheManager(PartitionManager.class.getResourceAsStream(ISPN_CONFIG));
            cacheManager.addListener(new TopologyChangeListener());
        } catch (IOException e) {
            log.error(e);
        }
    }

    public void stop() {
        cacheManager.stop();
        log.info("Stopping PartitionManager");
    }

    public void processTopologyChange() {
        log.info("New topology change");
        cacheManager.getMembers().stream().forEach(m -> log.info("{}", m));
    }

    /**
     * Auxiliary interface to add Infinispan listener to the caches
     */
    @Listener
    public class TopologyChangeListener {
        @ViewChanged
        public void onTopologyChange(ViewChangedEvent cacheEvent) {
            /*
                When a node is joining/leaving the cluster partition needs to be re-calculated and updated
             */
            processTopologyChange();
        }
    }
}
