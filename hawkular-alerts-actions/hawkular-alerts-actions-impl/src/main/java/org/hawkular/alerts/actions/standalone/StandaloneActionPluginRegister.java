/*
 * Copyright 2015-2016 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.alerts.actions.standalone;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.hawkular.alerts.actions.api.ActionPluginListener;
import org.hawkular.alerts.api.services.ActionListener;
import org.hawkular.alerts.api.services.ActionsService;
import org.hawkular.alerts.api.services.DefinitionsService;
import org.hawkular.alerts.engine.StandaloneAlerts;
import org.hawkular.alerts.log.MsgLogger;
import org.jboss.logging.Logger;

/**
 * Main standalone register for plugins implementations
 *
 * @author Lucas Ponce
 */
public class StandaloneActionPluginRegister {
    private static final MsgLogger log = Logger.getMessageLogger(MsgLogger.class,
            StandaloneActionPluginRegister.class.getName());

    private static StandaloneActionPluginRegister instance;

    private StandaloneActionPluginRegister() {
        init();
    }

    DefinitionsService definitions;
    ActionsService actions;

    Set<ActionListener> actionListeners = new HashSet<>();

    public void init() {
        definitions = StandaloneAlerts.getDefinitionsService();
        actions = StandaloneAlerts.getActionsService();
        Map<String, ActionPluginListener> plugins = ActionPlugins.getPlugins();
        for (String actionPlugin : plugins.keySet()) {
            ActionPluginListener actionPluginListener = plugins.get(actionPlugin);
            Set<String> properties = actionPluginListener.getProperties();
            Map<String, String> defaultProperties = actionPluginListener.getDefaultProperties();
            try {
                if (defaultProperties != null && !defaultProperties.isEmpty() ) {
                    definitions.addActionPlugin(actionPlugin, defaultProperties);
                } else {
                    definitions.addActionPlugin(actionPlugin, properties);
                }
            } catch (Exception e) {
                log.errorCannotRegisterPlugin(actionPlugin, e.getMessage());
            }
        }
        ActionListener actionListener = new StandaloneActionPluginListener(ActionPlugins.getPlugins());
        actions.addListener(actionListener);
        actionListeners.add(actionListener);
        log.info("Actions Plugins registration finished");
    }

    public static synchronized void start() {
        if (instance == null) {
            instance = new StandaloneActionPluginRegister();
        }
    }

    public static synchronized void stop() {
        if (instance != null && instance.actionListeners != null) {
            instance.actionListeners.stream().forEach(a -> {
                try {
                    if (a instanceof StandaloneActionPluginListener) {
                        ((StandaloneActionPluginListener)a).close();
                    }
                } catch (Exception e) {
                    log.debug(e.getMessage(), e);
                }
            });
        }
    }
}
