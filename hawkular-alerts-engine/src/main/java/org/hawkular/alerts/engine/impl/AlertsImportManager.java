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
package org.hawkular.alerts.engine.impl;

import java.io.File;
import java.util.List;

import org.hawkular.alerts.api.model.action.ActionDefinition;
import org.hawkular.alerts.api.model.export.Definitions;
import org.hawkular.alerts.api.model.trigger.FullTrigger;
import org.jboss.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Read a json file with a list of full triggers and actions definitions.
 *
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
public class AlertsImportManager {
    private static final Logger log = Logger.getLogger(AlertsImportManager.class);
    private ObjectMapper objectMapper = new ObjectMapper();
    private Definitions definitions;

    /**
     * Read a json file and initialize the AlertsImportManager instance
     *
     * @param fAlerts json file to read
     * @throws Exception on any problem
     */
    public AlertsImportManager(File fAlerts) throws Exception {
        if (fAlerts == null) {
            throw new IllegalArgumentException("fAlerts must be not null");
        }
        if (!fAlerts.exists() || !fAlerts.isFile()) {
            throw new IllegalArgumentException(fAlerts.getName() + " file must exist");
        }

        definitions = objectMapper.readValue(fAlerts, Definitions.class);
        if (log.isDebugEnabled()) {
            if (definitions != null) {
                log.debug("File: " + fAlerts.toString() + " imported in " + definitions.toString());
            } else {
                log.debug("File: " + fAlerts.toString() + " imported is null");
            }
        }
    }

    public List<FullTrigger> getFullTriggers() {
        return (definitions != null ? definitions.getTriggers() : null);
    }

    public List<ActionDefinition> getActionDefinitions() {
        return (definitions != null ? definitions.getActions() : null);
    }

}
