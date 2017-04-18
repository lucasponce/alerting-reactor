package org.hawkular.alerts.netty.handlers;

import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static org.hawkular.alerts.api.json.JsonUtil.fromJson;
import static org.hawkular.alerts.netty.HandlersManager.TENANT_HEADER_NAME;
import static org.hawkular.alerts.netty.util.ResponseUtil.badRequest;
import static org.hawkular.alerts.netty.util.ResponseUtil.extractPaging;
import static org.hawkular.alerts.netty.util.ResponseUtil.internalServerError;
import static org.hawkular.alerts.netty.util.ResponseUtil.isEmpty;
import static org.hawkular.alerts.netty.util.ResponseUtil.notFound;
import static org.hawkular.alerts.netty.util.ResponseUtil.ok;
import static org.hawkular.alerts.netty.util.ResponseUtil.paginatedOk;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hawkular.alerts.api.model.action.Action;
import org.hawkular.alerts.api.model.action.ActionDefinition;
import org.hawkular.alerts.api.model.paging.Page;
import org.hawkular.alerts.api.model.paging.Pager;
import org.hawkular.alerts.api.services.ActionsCriteria;
import org.hawkular.alerts.api.services.ActionsService;
import org.hawkular.alerts.api.services.DefinitionsService;
import org.hawkular.alerts.engine.StandaloneAlerts;
import org.hawkular.alerts.log.MsgLogger;
import org.hawkular.alerts.netty.RestEndpoint;
import org.hawkular.alerts.netty.RestHandler;
import org.jboss.logging.Logger;
import org.reactivestreams.Publisher;

import io.netty.handler.codec.http.HttpMethod;
import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.server.HttpServerResponse;

/**
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
@RestEndpoint(path = "/actions")
public class ActionsHandler implements RestHandler {
    private static final MsgLogger log = Logger.getMessageLogger(MsgLogger.class, ActionsHandler.class.getName());
    private static final String ROOT = "/";
    private static final String HISTORY = "/history";
    private static final String HISTORY_DELETE = "/history/delete";
    private static final String PLUGIN = "/plugin";
    private static final String PARAM_START_TIME = "startTime";
    private static final String PARAM_END_TIME = "endTime";
    private static final String PARAM_ACTION_PLUGINS = "actionPlugins";
    private static final String PARAM_ACTION_IDS = "actionIds";
    private static final String PARAM_ALERTS_IDS = "alertIds";
    private static final String PARAM_RESULTS = "results";
    private static final String COMMA = ",";

    ActionsService actionsService;
    DefinitionsService definitionsService;

    public ActionsHandler() {
        actionsService = StandaloneAlerts.getActionsService();
        definitionsService = StandaloneAlerts.getDefinitionsService();
    }

    @Override
    public Publisher<Void> process(HttpServerRequest req,
                                   HttpServerResponse resp,
                                   String tenantId,
                                   String subpath,
                                   Map<String, List<String>> params) {
        HttpMethod method = req.method();
        if (isEmpty(tenantId)) {
            return badRequest(resp, TENANT_HEADER_NAME + " header is required");
        }
        // GET /
        if (method == GET && subpath.equals(ROOT)) {
            return findActionIds(resp, tenantId);
        }
        // POST /
        // PUT /
        if ((method == POST || method == PUT) && subpath.equals(ROOT)) {
            ActionDefinition actionDefinition = null;
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            try {
                actionDefinition = fromJson(json, ActionDefinition.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing ActionDefinition json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            if (actionDefinition == null) {
                return badRequest(resp, "actionDefinition must be not null");
            }
            if (isEmpty(actionDefinition.getActionPlugin())) {
                return badRequest(resp, "actionPlugin must be not null");
            }
            if (isEmpty(actionDefinition.getActionId())) {
                return badRequest(resp, "actionId must be not null");
            }
            if (isEmpty(actionDefinition.getProperties())) {
                return badRequest(resp, "properties must be not null");
            }
            actionDefinition.setTenantId(tenantId);
            if (method == POST) {
                return createActionDefinition(resp, actionDefinition);
            } else {
                return updateActionDefinition(resp, actionDefinition);
            }
        }
        // GET /history
        if (method == GET && subpath.equals(HISTORY)) {
            return findActionsHistory(req, resp, tenantId, params, req.uri());
        }
        // PUT /history/delete
        if (method == PUT && subpath.equals(HISTORY_DELETE)) {
            return deleteActionsHistory(resp, tenantId, params);
        }
        String[] tokens = subpath.substring(1).split(ROOT);
        // GET /plugin/{actionPlugin}
        if (method == GET && subpath.startsWith(PLUGIN) && tokens.length == 2) {
            return findActionIdsByPlugin(resp, tenantId, tokens[1]);
        }
        // GET /{actionPlugin}/{actionId}
        if (method == GET && tokens.length == 2) {
            return getActionDefinition(resp, tenantId, tokens[0], tokens[1]);
        }
        // DELETE /{actionPlugin}/{actionId}
        if (method == DELETE && tokens.length == 2) {
            return deleteActionDefinition(resp, tenantId, tokens[0], tokens[1]);
        }
        return badRequest(resp, "Wrong path " + method + " " + subpath);
    }

    Publisher<Void> findActionIds(HttpServerResponse resp, String tenantId) {
        try {
            Map<String, Set<String>> actions = definitionsService.getActionDefinitionIds(tenantId);
            log.debugf("Actions: %s", actions);
            return ok(resp, actions);
        } catch (Exception e) {
            log.errorf(e, "Error querying actions ids for tenantId %s. Reason: %s", tenantId, e.toString());
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> findActionIdsByPlugin(HttpServerResponse resp, String tenantId, String actionPlugin) {
        try {
            Collection<String> actions = definitionsService.getActionDefinitionIds(tenantId, actionPlugin);
            log.debugf("Actions: %s", actions);
            return ok(resp, actions);
        } catch (Exception e) {
            log.errorf(e, "Error querying actions ids for tenantId %s and actionPlugin %s. Reason: %s", tenantId, actionPlugin, e.toString());
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> getActionDefinition(HttpServerResponse resp, String tenantId, String actionPlugin, String actionId) {
        try {
            ActionDefinition actionDefinition = definitionsService.getActionDefinition(tenantId, actionPlugin, actionId);
            log.debugf("ActionDefinition: %s", actionDefinition);
            if (actionDefinition == null) {
                return notFound(resp, "Not action found for actionPlugin: " + actionPlugin + " and actionId: " + actionId);
            }
            return ok(resp, actionDefinition);
        } catch (Exception e) {
            log.errorf("Error querying action definition for tenantId %s actionPlugin %s and actionId %s", tenantId, actionPlugin, actionId);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> createActionDefinition(HttpServerResponse resp, ActionDefinition actionDefinition) {
        try {
            if (definitionsService.getActionDefinition(actionDefinition.getTenantId(),
                    actionDefinition.getActionPlugin(), actionDefinition.getActionId()) != null) {
                return badRequest(resp, "Existing ActionDefinition: " + actionDefinition);
            } else {
                definitionsService.addActionDefinition(actionDefinition.getTenantId(), actionDefinition);
                log.debugf("ActionDefinition: %s", actionDefinition);
                return ok(resp, actionDefinition);
            }
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            if (e.getCause() != null && e.getCause() instanceof IllegalArgumentException) {
                return badRequest(resp,"Bad arguments: " + e.getMessage());
            }
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> updateActionDefinition(HttpServerResponse resp, ActionDefinition actionDefinition) {
        try {
            if (definitionsService.getActionDefinition(actionDefinition.getTenantId(),
                    actionDefinition.getActionPlugin(), actionDefinition.getActionId()) != null) {
                definitionsService.updateActionDefinition(actionDefinition.getTenantId(), actionDefinition);
                log.debugf("ActionDefinition: %s", actionDefinition);
                return ok(resp, actionDefinition);
            } else {
                return notFound(resp, "ActionDefinition: " + actionDefinition + " not found for update");
            }
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            if (e.getCause() != null && e.getCause() instanceof IllegalArgumentException) {
                return badRequest(resp,"Bad arguments: " + e.getMessage());
            }
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> deleteActionDefinition(HttpServerResponse resp, String tenantId, String actionPlugin, String actionId) {
        try {
            if (definitionsService.getActionDefinition(tenantId, actionPlugin, actionId) != null) {
                definitionsService.removeActionDefinition(tenantId, actionPlugin, actionId);
                log.debugf("ActionPlugin: %s ActionId: %s", actionPlugin, actionId);
                return ok(resp);
            } else {
                return notFound(resp, "ActionPlugin: " + actionPlugin + " ActionId: " + actionId +
                        " not found for delete");
            }
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> findActionsHistory(HttpServerRequest req, HttpServerResponse resp, String tenantId, Map<String, List<String>> params, String uri) {
        try {
            Pager pager = extractPaging(params);
            ActionsCriteria criteria = buildCriteria(params);
            Page<Action> actionPage = actionsService.getActions(tenantId, criteria, pager);
            log.debugf("Actions: %s", actionPage);
            if (isEmpty(actionPage)) {
                return ok(resp, actionPage);
            }
            return paginatedOk(req, resp, actionPage, uri);
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            if (e.getCause() != null && e.getCause() instanceof IllegalArgumentException) {
                return badRequest(resp,"Bad arguments: " + e.getMessage());
            }
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> deleteActionsHistory(HttpServerResponse resp, String tenantId, Map<String, List<String>> params) {
        try {
            ActionsCriteria criteria = buildCriteria(params);
            int numDeleted = actionsService.deleteActions(tenantId, criteria);
            log.debugf("Actions deleted: %s", numDeleted);
            Map<String, String> deleted = new HashMap<>();
            deleted.put("deleted", String.valueOf(numDeleted));
            return ok(resp, deleted);
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            if (e.getCause() != null && e.getCause() instanceof IllegalArgumentException) {
                return badRequest(resp,"Bad arguments: " + e.getMessage());
            }
            return internalServerError(resp, e.toString());
        }
    }

    ActionsCriteria buildCriteria(Map<String, List<String>> params) {
        ActionsCriteria criteria = new ActionsCriteria();
        if (params.get(PARAM_START_TIME) != null) {
            criteria.setStartTime(Long.valueOf(params.get(PARAM_START_TIME).get(0)));
        }
        if (params.get(PARAM_END_TIME) != null) {
            criteria.setEndTime(Long.valueOf(params.get(PARAM_END_TIME).get(0)));
        }
        if (params.get(PARAM_ACTION_PLUGINS) != null) {
            criteria.setActionPlugins(Arrays.asList(params.get(PARAM_ACTION_PLUGINS).get(0).split(COMMA)));
        }
        if (params.get(PARAM_ACTION_IDS) != null) {
            criteria.setActionIds(Arrays.asList(params.get(PARAM_ACTION_IDS).get(0).split(COMMA)));
        }
        if (params.get(PARAM_ALERTS_IDS) != null) {
            criteria.setAlertIds(Arrays.asList(params.get(PARAM_ALERTS_IDS).get(0).split(COMMA)));
        }
        if (params.get(PARAM_RESULTS) != null) {
            criteria.setResults(Arrays.asList(params.get(PARAM_RESULTS).get(0).split(COMMA)));
        }
        return criteria;
    }
}
