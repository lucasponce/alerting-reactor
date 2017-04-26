package org.hawkular.alerts.netty.handlers;

import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static org.hawkular.alerts.api.json.JsonUtil.collectionFromJson;
import static org.hawkular.alerts.api.json.JsonUtil.fromJson;
import static org.hawkular.alerts.netty.HandlersManager.TENANT_HEADER_NAME;
import static org.hawkular.alerts.netty.util.ResponseUtil.badRequest;
import static org.hawkular.alerts.netty.util.ResponseUtil.checkTags;
import static org.hawkular.alerts.netty.util.ResponseUtil.extractPaging;
import static org.hawkular.alerts.netty.util.ResponseUtil.getCleanDampening;
import static org.hawkular.alerts.netty.util.ResponseUtil.internalServerError;
import static org.hawkular.alerts.netty.util.ResponseUtil.isEmpty;
import static org.hawkular.alerts.netty.util.ResponseUtil.notFound;
import static org.hawkular.alerts.netty.util.ResponseUtil.ok;
import static org.hawkular.alerts.netty.util.ResponseUtil.paginatedOk;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.hawkular.alerts.api.exception.NotFoundException;
import org.hawkular.alerts.api.json.GroupConditionsInfo;
import org.hawkular.alerts.api.json.GroupMemberInfo;
import org.hawkular.alerts.api.json.UnorphanMemberInfo;
import org.hawkular.alerts.api.model.condition.Condition;
import org.hawkular.alerts.api.model.dampening.Dampening;
import org.hawkular.alerts.api.model.paging.Page;
import org.hawkular.alerts.api.model.paging.Pager;
import org.hawkular.alerts.api.model.trigger.FullTrigger;
import org.hawkular.alerts.api.model.trigger.Mode;
import org.hawkular.alerts.api.model.trigger.Trigger;
import org.hawkular.alerts.api.services.DefinitionsService;
import org.hawkular.alerts.api.services.TriggersCriteria;
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
@RestEndpoint(path = "/triggers")
public class TriggersHandler implements RestHandler {
    private static final MsgLogger log = Logger.getMessageLogger(MsgLogger.class, TriggersHandler.class.getName());
    private static final String ROOT = "/";
    private static final String GROUPS = "groups";
    private static final String MEMBERS = "members";
    private static final String TRIGGER = "trigger";
    private static final String ORPHAN = "orphan";
    private static final String UNORPHAN = "unorphan";
    private static final String DAMPENINGS = "dampenings";
    private static final String MODE = "mode";
    private static final String CONDITIONS = "conditions";
    private static final String ENABLED = "enabled";
    private static final String PARAM_KEEP_NON_ORPHANS = "keepNonOrphans";
    private static final String PARAM_KEEP_ORPHANS = "keepOrphans";
    private static final String PARAM_INCLUDE_ORPHANS = "includeOrphans";
    private static final String PARAM_TRIGGER_IDS = "triggerIds";
    private static final String PARAM_TAGS = "tags";
    private static final String PARAM_THIN = "thin";
    private static final String PARAM_ENABLED = "enabled";

    DefinitionsService definitionsService;

    public TriggersHandler() {
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

        String[] tokens = subpath.substring(1).split(ROOT);

        // GET /
        if (method == GET && subpath.equals(ROOT)) {
            return findTriggers(req, resp, tenantId, params, req.uri());
        }
        // POST /
        if (method == POST && subpath.equals(ROOT)) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            Trigger trigger;
            try {
                trigger = fromJson(json, Trigger.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing Trigger json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return createTrigger(resp, tenantId, trigger, false);
        }

        // GET /{triggerId}
        if (method == GET && tokens.length == 1) {
            return getTrigger(resp, tenantId, tokens[0], false);
        }
        // POST /trigger
        if (method == POST && tokens.length == 1 && TRIGGER.equals(tokens[0])) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            FullTrigger fullTrigger;
            try {
                fullTrigger = fromJson(json, FullTrigger.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing FullTrigger json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return createFullTrigger(resp, tenantId, fullTrigger);
        }
        // POST /groups
        if (method == POST && tokens.length == 1 && GROUPS.equals(tokens[0])) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            Trigger trigger;
            try {
                trigger = fromJson(json, Trigger.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing Trigger json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return createTrigger(resp, tenantId, trigger, true);
        }
        // PUT /enabled
        if (method == PUT && tokens.length == 1 && ENABLED.equals(tokens[0])) {
            return setTriggersEnabled(resp, tenantId, params, false);
        }
        // PUT /{triggerId}
        if (method == PUT && tokens.length == 1) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            Trigger trigger;
            try {
                trigger = fromJson(json, Trigger.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing Trigger json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return updateTrigger(resp, tenantId, tokens[0], trigger, false);
        }
        // DELETE /{triggerId}
        if (method == DELETE && tokens.length == 1) {
            return deleteTrigger(resp, tenantId, tokens[0]);
        }

        // GET /trigger/{triggerId}
        if (method == GET && tokens.length == 2 && TRIGGER.equals(tokens[0])) {
            return getTrigger(resp, tenantId, tokens[1], true);
        }
        // GET /{triggerId}/dampenings
        if (method == GET && tokens.length ==2 && DAMPENINGS.equals(tokens[1])) {
            return getTriggerDampenings(resp, tenantId, tokens[0], null);
        }
        // GET /{triggerId}/conditions
        if (method == GET && tokens.length == 2 && CONDITIONS.equals(tokens[1])) {
            return getTriggerConditions(resp, tenantId, tokens[0]);
        }
        // POST /groups/members
        if (method == POST && tokens.length == 2 && GROUPS.equals(tokens[0]) && MEMBERS.equals(tokens[1])) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            GroupMemberInfo groupMember;
            try {
                groupMember = fromJson(json, GroupMemberInfo.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing GroupMemberInfo json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return createGroupMember(resp, tenantId, groupMember);
        }
        // POST /{triggerId}/dampenings
        if (method == POST && tokens.length == 2 && DAMPENINGS.equals(tokens[1])) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            Dampening dampening;
            try {
                dampening = fromJson(json, Dampening.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing Dampening json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return createDampening(resp, tenantId, tokens[0], dampening, false);
        }
        // PUT /groups/enabled
        if (method == PUT && tokens.length == 2 && GROUPS.equals(tokens[0]) && ENABLED.equals(tokens[1])) {
            return setTriggersEnabled(resp, tenantId, params, true);
        }
        // PUT /groups/{groupId}
        if (method == PUT && tokens.length == 2 && GROUPS.equals(tokens[0])) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            Trigger trigger;
            try {
                trigger = fromJson(json, Trigger.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing Trigger json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return updateTrigger(resp, tenantId, tokens[0], trigger, true);
        }
        // PUT /{triggerId}/conditions
        if (method == PUT && tokens.length == 2 && CONDITIONS.equals(tokens[1])) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            Collection<Condition> conditions;
            try {
                conditions = collectionFromJson(json, Condition.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing Condition json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return setConditions(resp, tenantId, tokens[0], null, conditions);
        }
        // DELETE /groups/{groupId}
        if (method == DELETE && tokens.length == 2 && GROUPS.equals(tokens[0])) {
            return deleteGroupTrigger(resp, tenantId, tokens[0], params);
        }

        // GET /{triggerId}/dampenings/{dampeningId}
        if (method == GET && tokens.length == 3 && DAMPENINGS.equals(tokens[1])) {
            return getDampening(resp, tenantId, tokens[0], tokens[2]);
        }
        // GET /groups/{groupId}/members
        if (method == GET && tokens.length == 3 && GROUPS.equals(tokens[0]) && MEMBERS.equals(tokens[2])) {
            return findGroupMembers(resp, tenantId, tokens[1], params);
        }
        // POST /groups/{groupId}/dampenings
        if (method == POST && tokens.length == 3 && GROUPS.equals(tokens[0]) && DAMPENINGS.equals(tokens[2])) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            Dampening dampening;
            try {
                dampening = fromJson(json, Dampening.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing Dampening json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return createDampening(resp, tenantId, tokens[1], dampening, true);
        }
        // PUT /{triggerId}/dampenings/{dampeningId}
        if (method == PUT && tokens.length == 3 && DAMPENINGS.equals(tokens[1])) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            Dampening dampening;
            try {
                dampening = fromJson(json, Dampening.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing Dampening json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return updateDampening(resp, tenantId, tokens[0], tokens[2], dampening, false);
        }
        // PUT /{triggerId}/conditions/{triggerMode}
        if (method == PUT && tokens.length == 3 && CONDITIONS.equals(tokens[1])) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            Collection<Condition> conditions;
            try {
                conditions = collectionFromJson(json, Condition.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing Condition json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return setConditions(resp, tenantId, tokens[0], tokens[2], conditions);
        }
        // PUT /groups/{groupId}/conditions
        if (method == PUT && tokens.length == 3 && GROUPS.equals(tokens[0]) && CONDITIONS.equals(tokens[2])) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            GroupConditionsInfo groupConditionsInfo;
            try {
                groupConditionsInfo = fromJson(json, GroupConditionsInfo.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing GroupConditionsInfo json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return setGroupConditions(resp, tenantId, tokens[0], null, groupConditionsInfo);
        }
        // DELETE /{triggerId}/dampenings/{dampeningId}
        if (method == DELETE && tokens.length == 3 && DAMPENINGS.equals(tokens[1])) {
            return deleteDampening(resp, tenantId, tokens[0], tokens[2], false);
        }

        // GET /{triggerId}/dampenings/mode/{triggerMode}
        if (method == GET && tokens.length == 4 && DAMPENINGS.equals(tokens[1]) && MODE.equals(tokens[2])) {
            return getTriggerDampenings(resp, tenantId, tokens[0], Mode.valueOf(tokens[3]));
        }
        // POST /groups/members/{memberId}/orphan
        if (method == POST && tokens.length == 4 && GROUPS.equals(tokens[0]) && MEMBERS.equals(tokens[1]) && ORPHAN.equals(tokens[3])) {
            return orphanMemberTrigger(resp, tenantId, tokens[2]);
        }
        // POST /groups/members/{memberId}/unorphan
        if (method == POST && tokens.length == 4 && GROUPS.equals(tokens[0]) && MEMBERS.equals(tokens[1]) && UNORPHAN.equals(tokens[3])) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            UnorphanMemberInfo unorphanMemberInfo;
            try {
                unorphanMemberInfo = fromJson(json, UnorphanMemberInfo.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing UnorphanMemberInfo json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return unorphanMemberTrigger(resp, tenantId, tokens[2], unorphanMemberInfo);
        }
        // PUT /groups/{groupId}/dampenings/{dampeningId}
        if (method == PUT && tokens.length == 4 && GROUPS.equals(tokens[0]) && DAMPENINGS.equals(tokens[2])) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            Dampening dampening;
            try {
                dampening = fromJson(json, Dampening.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing Dampening json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return updateDampening(resp, tenantId, tokens[0], tokens[2], dampening, true);
        }
        // PUT /groups/{groupId}/conditions/{triggerMode}
        if (method == PUT && tokens.length == 4 && GROUPS.equals(tokens[1]) && CONDITIONS.equals(tokens[3])) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            GroupConditionsInfo groupConditionsInfo;
            try {
                groupConditionsInfo = fromJson(json, GroupConditionsInfo.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing GroupConditionsInfo json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return setGroupConditions(resp, tenantId, tokens[0], tokens[2], groupConditionsInfo);
        }
        // DELETE /groups/{groupId}/dampenings/{dampeningId}
        if (method == DELETE && tokens.length == 4 && GROUPS.equals(tokens[0]) && DAMPENINGS.equals(tokens[2])) {
            return deleteDampening(resp, tenantId, tokens[1], tokens[3], true);
        }

        return badRequest(resp, "Wrong path " + method + " " + subpath);
    }

    Publisher<Void> createDampening(HttpServerResponse resp, String tenantId, String triggerId, Dampening dampening, boolean isGroupTrigger) {
        try {
            dampening.setTenantId(tenantId);
            dampening.setTriggerId(triggerId);
            boolean exists = (definitionsService.getDampening(tenantId, dampening.getDampeningId()) != null);
            if (!exists) {
                Dampening d = getCleanDampening(dampening);
                if (!isGroupTrigger) {
                    definitionsService.addDampening(tenantId, d);
                } else {
                    definitionsService.addGroupDampening(tenantId, d);
                }
                log.debugf("Dampening: %s", dampening);
                return ok(resp, d);
            }
            return badRequest(resp,"Existing dampening for dampeningId: " + dampening.getDampeningId());
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> createFullTrigger(HttpServerResponse resp, String tenantId, FullTrigger fullTrigger) {
        try {
            if (fullTrigger.getTrigger() == null) {
                return badRequest(resp,"Trigger is empty");
            }
            Trigger trigger = fullTrigger.getTrigger();
            trigger.setTenantId(tenantId);
            if (isEmpty(trigger.getId())) {
                trigger.setId(Trigger.generateId());
            } else if (definitionsService.getTrigger(tenantId, trigger.getId()) != null) {
                return badRequest(resp, "Trigger with ID [" + trigger.getId() + "] exists.");
            }
            if (!checkTags(trigger)) {
                return badRequest(resp,"Tags " + trigger.getTags() + " must be non empty.");
            }
            definitionsService.addTrigger(tenantId, trigger);
            log.debugf("Trigger: %s", trigger);
            for (Dampening dampening : fullTrigger.getDampenings()) {
                dampening.setTenantId(tenantId);
                dampening.setTriggerId(trigger.getId());
                boolean exist = (definitionsService.getDampening(tenantId, dampening.getDampeningId()) != null);
                if (exist) {
                    definitionsService.removeDampening(tenantId, dampening.getDampeningId());
                }
                definitionsService.addDampening(tenantId, dampening);
                log.debugf("Dampening: %s", dampening);
            }
            fullTrigger.getConditions().stream().forEach(c -> {
                c.setTenantId(tenantId);
                c.setTriggerId(trigger.getId());
            });
            List<Condition> firingConditions = fullTrigger.getConditions().stream()
                    .filter(c -> c.getTriggerMode() == Mode.FIRING)
                    .collect(Collectors.toList());
            if (firingConditions != null && !firingConditions.isEmpty()) {
                definitionsService.setConditions(tenantId, trigger.getId(), Mode.FIRING, firingConditions);
                log.debugf("Conditions: %s", firingConditions);
            }
            List<Condition> autoResolveConditions = fullTrigger.getConditions().stream()
                    .filter(c -> c.getTriggerMode() == Mode.AUTORESOLVE)
                    .collect(Collectors.toList());
            if (autoResolveConditions != null && !autoResolveConditions.isEmpty()) {
                definitionsService.setConditions(tenantId, trigger.getId(), Mode.AUTORESOLVE, autoResolveConditions);
                log.debugf("Conditions: %s", autoResolveConditions);
            }
            return ok(resp, fullTrigger);
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> createGroupMember(HttpServerResponse resp, String tenantId, GroupMemberInfo groupMember) {
        try {
            String groupId = groupMember.getGroupId();
            if (isEmpty(groupId)) {
                return badRequest(resp, "MemberTrigger groupId is null");
            }
            if (!checkTags(groupMember)) {
                return badRequest(resp, "Tags " + groupMember.getMemberTags() + " must be non empty.");
            }
            Trigger child = definitionsService.addMemberTrigger(tenantId, groupId, groupMember.getMemberId(),
                    groupMember.getMemberName(),
                    groupMember.getMemberDescription(),
                    groupMember.getMemberContext(),
                    groupMember.getMemberTags(),
                    groupMember.getDataIdMap());
            log.debugf("Child Trigger: %s", child);
            return ok(resp, child);
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> createTrigger(HttpServerResponse resp, String tenantId, Trigger trigger, boolean isGroupTrigger) {
        try {
            if (isEmpty(trigger.getId())) {
                trigger.setId(Trigger.generateId());
            } else if (definitionsService.getTrigger(tenantId, trigger.getId()) != null) {
                return badRequest(resp, "Trigger with ID [" + trigger.getId() + "] exists.");
            }
            if (!checkTags(trigger)) {
                return badRequest(resp, "Tags " + trigger.getTags() + " must be non empty.");
            }
            definitionsService.addTrigger(tenantId, trigger);
            log.debugf("Trigger: %s", trigger);
            return ok(resp, trigger);

        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> deleteDampening(HttpServerResponse resp, String tenantId, String triggerId, String dampeningId, boolean isGroupTrigger) {
        try {
            boolean exists = (definitionsService.getDampening(tenantId, dampeningId) != null);
            if (exists) {
                if (!isGroupTrigger) {
                    definitionsService.removeDampening(tenantId, dampeningId);
                } else {
                    definitionsService.removeGroupDampening(tenantId, dampeningId);
                }
                log.debugf("DampeningId: %s", dampeningId);
                return ok(resp);
            }
            return notFound(resp, "Dampening " + dampeningId + " not found for triggerId: " + triggerId);
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> deleteGroupTrigger(HttpServerResponse resp, String tenantId, String groupId, Map<String, List<String>> params) {
        try {
            boolean keepNonOrphans = false;
            if (params.get(PARAM_KEEP_NON_ORPHANS) != null) {
                keepNonOrphans = Boolean.valueOf(params.get(PARAM_KEEP_NON_ORPHANS).get(0));
            }
            boolean keepOrphans = false;
            if (params.get(PARAM_KEEP_ORPHANS) != null) {
                keepOrphans = Boolean.valueOf(params.get(PARAM_KEEP_ORPHANS).get(0));
            }
            definitionsService.removeGroupTrigger(tenantId, groupId, keepNonOrphans, keepOrphans);
            if (log.isDebugEnabled()) {
                log.debugf("Remove Group Trigger: %s / %s", tenantId, groupId);
            }
            return ok(resp);
        } catch (NotFoundException e) {
            return notFound(resp, e.getMessage());
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> deleteTrigger(HttpServerResponse resp, String tenantId, String triggerId) {
        try {
            definitionsService.removeTrigger(tenantId, triggerId);
            log.debugf("TriggerId: %s", triggerId);
            return ok(resp);
        } catch (NotFoundException e) {
            return notFound(resp, e.getMessage());
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> findGroupMembers(HttpServerResponse resp, String tenantId, String groupId, Map<String, List<String>> params) {
        try {
            boolean includeOrphans = false;
            if (params.get(PARAM_INCLUDE_ORPHANS) != null) {
                includeOrphans = Boolean.valueOf(params.get(PARAM_INCLUDE_ORPHANS).get(0));
            }
            Collection<Trigger> members = definitionsService.getMemberTriggers(tenantId, groupId, includeOrphans);
            log.debugf("Member Triggers: %s", members);
            return ok(resp, members);
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> findTriggers(HttpServerRequest req, HttpServerResponse resp, String tenantId, Map<String, List<String>> params, String uri) {
        try {
            Pager pager = extractPaging(params);
            TriggersCriteria criteria = buildCriteria(params);
            Page<Trigger> triggerPage = definitionsService.getTriggers(tenantId, criteria, pager);
            log.debugf("Triggers: %s", triggerPage);
            if (isEmpty(triggerPage)) {
                return ok(resp, triggerPage);
            }
            return paginatedOk(req, resp, triggerPage, uri);
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> getDampening(HttpServerResponse resp, String tenantId, String triggerId, String dampeningId) {
        try {
            Dampening found = definitionsService.getDampening(tenantId, dampeningId);
            if (found == null) {
                return notFound(resp, "No dampening found for triggerId: " + triggerId + " and dampeningId:" +
                        dampeningId);
            }
            return ok(resp, found);
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> getTrigger(HttpServerResponse resp, String tenantId, String triggerId, boolean isFullTrigger) {
        try {
            Trigger found = definitionsService.getTrigger(tenantId, triggerId);
            if (found == null) {
                return notFound(resp, "triggerId: " + triggerId + " not found");
            }
            log.debugf("Trigger: %s", found);
            if (isFullTrigger) {
                    List<Dampening> dampenings = new ArrayList<>(definitionsService.getTriggerDampenings(tenantId, found.getId(), null));
                    List<Condition> conditions = new ArrayList<>(definitionsService.getTriggerConditions(tenantId, found.getId(), null));
                    FullTrigger fullTrigger = new FullTrigger(found, dampenings, conditions);
                    return ok(resp, fullTrigger);
            }
            return ok(resp, found);
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> getTriggerConditions(HttpServerResponse resp, String tenantId, String triggerId) {
        try {
            Collection<Condition> conditions = definitionsService.getTriggerConditions(tenantId, triggerId, null);
            log.debugf("Conditions: %s", conditions);
            return ok(resp, conditions);
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> getTriggerDampenings(HttpServerResponse resp, String tenantId, String triggerId, Mode triggerMode) {
        try {
            Collection<Dampening> dampenings = definitionsService.getTriggerDampenings(tenantId, triggerId, triggerMode);
            log.debug("Dampenings: " + dampenings);
            return ok(resp, dampenings);
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> updateTrigger(HttpServerResponse resp, String tenantId, String triggerId, Trigger trigger, boolean isGroupTrigger) {
        try {
            if (trigger != null && !isEmpty(triggerId)) {
                trigger.setId(triggerId);
            }
            if (!checkTags(trigger)) {
                return badRequest(resp, "Tags " + trigger.getTags() + " must be non empty.");
            }
            if (isGroupTrigger) {
                definitionsService.updateGroupTrigger(tenantId, trigger);
            } else {
                definitionsService.updateTrigger(tenantId, trigger);
            }
            log.debugf("Trigger: %s", trigger);
            return ok(resp);
        } catch (NotFoundException e) {
            return notFound(resp, e.getMessage());
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> orphanMemberTrigger(HttpServerResponse resp, String tenantId, String memberId) {
        try {
            Trigger child = definitionsService.orphanMemberTrigger(tenantId, memberId);
            log.debugf("Orphan Member Trigger: %s", child);
            return ok(resp);
        } catch (NotFoundException e) {
            return notFound(resp, e.getMessage());
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> updateDampening(HttpServerResponse resp, String tenantId, String triggerId, String dampeningId, Dampening dampening, boolean isGroupTrigger) {
        try {
            boolean exists = (definitionsService.getDampening(tenantId, dampeningId) != null);
            if (exists) {
                dampening.setTriggerId(triggerId);
                Dampening d = getCleanDampening(dampening);
                log.debugf("Dampening: %s", d);
                if (isGroupTrigger) {
                    definitionsService.updateGroupDampening(tenantId, d);
                } else {
                    definitionsService.updateDampening(tenantId, d);
                }
            }
            return notFound(resp, "No dampening found for dampeningId: " + dampeningId);
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> unorphanMemberTrigger(HttpServerResponse resp, String tenantId, String memberId, UnorphanMemberInfo unorphanMemberInfo) {
        try {
            if (!checkTags(unorphanMemberInfo)) {
                return badRequest(resp, "Tags " + unorphanMemberInfo.getMemberTags() + " must be non empty.");
            }
            Trigger child = definitionsService.unorphanMemberTrigger(tenantId, memberId,
                    unorphanMemberInfo.getMemberContext(),
                    unorphanMemberInfo.getMemberTags(),
                    unorphanMemberInfo.getDataIdMap());
            log.debugf("Member Trigger: %s",child);
            return ok(resp);
        } catch (NotFoundException e) {
            return notFound(resp, e.getMessage());
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> setConditions(HttpServerResponse resp, String tenantId, String triggerId, String triggerMode, Collection<Condition> conditions) {
        try {
            Collection<Condition> updatedConditions;
            if (triggerMode == null) {
                updatedConditions = new HashSet<>();
                conditions.stream().forEach(c -> c.setTriggerId(triggerId));
                Collection<Condition> firingConditions = conditions.stream()
                        .filter(c -> c.getTriggerMode() == null || c.getTriggerMode().equals(Mode.FIRING))
                        .collect(Collectors.toList());
                updatedConditions.addAll(definitionsService.setConditions(tenantId, triggerId, Mode.FIRING, firingConditions));
                Collection<Condition> autoResolveConditions = conditions.stream()
                        .filter(c -> c.getTriggerMode().equals(Mode.AUTORESOLVE))
                        .collect(Collectors.toList());
                updatedConditions.addAll(definitionsService.setConditions(tenantId, triggerId, Mode.AUTORESOLVE,
                        autoResolveConditions));
                log.debugf("Conditions: %s", updatedConditions);
                return ok(resp, updatedConditions);
            }
            Mode mode = Mode.valueOf(triggerMode.toUpperCase());
            if (!isEmpty(conditions)) {
                for (Condition condition : conditions) {
                    condition.setTriggerId(triggerId);
                    if (condition.getTriggerMode() == null || !condition.getTriggerMode().equals(mode)) {
                        return badRequest(resp,"Condition: " + condition + " has a different triggerMode [" + triggerMode + "]");
                    }
                }
            }
            updatedConditions = definitionsService.setConditions(tenantId, triggerId, mode, conditions);
            return ok(resp, updatedConditions);
        } catch (NotFoundException e) {
            return notFound(resp, e.getMessage());
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> setGroupConditions(HttpServerResponse resp, String tenantId, String groupId, String triggerMode, GroupConditionsInfo groupConditionsInfo) {
        try {
            Collection<Condition> updatedConditions = new HashSet<>();
            if (groupConditionsInfo == null) {
                return badRequest(resp, "GroupConditionsInfo must be non null.");
            }
            if (groupConditionsInfo.getConditions() == null) {
                groupConditionsInfo.setConditions(Collections.EMPTY_LIST);
            }
            for (Condition condition : groupConditionsInfo.getConditions()) {
                if (condition == null) {
                    return badRequest(resp,"GroupConditionsInfo must have non null conditions: " + groupConditionsInfo);
                }
                condition.setTriggerId(groupId);
            }
            if (triggerMode == null) {
                Collection<Condition> firingConditions = groupConditionsInfo.getConditions().stream()
                        .filter(c -> c.getTriggerMode() == null || c.getTriggerMode().equals(Mode.FIRING))
                        .collect(Collectors.toList());
                updatedConditions.addAll(definitionsService.setGroupConditions(tenantId, groupId, Mode.FIRING, firingConditions,
                        groupConditionsInfo.getDataIdMemberMap()));
                Collection<Condition> autoResolveConditions = groupConditionsInfo.getConditions().stream()
                        .filter(c -> c.getTriggerMode().equals(Mode.AUTORESOLVE))
                        .collect(Collectors.toList());
                updatedConditions.addAll(definitionsService.setGroupConditions(tenantId, groupId, Mode.AUTORESOLVE,
                        autoResolveConditions,
                        groupConditionsInfo.getDataIdMemberMap()));
                log.debugf("Conditions: %s", updatedConditions);
                return ok(resp, updatedConditions);
            }
            Mode mode = Mode.valueOf(triggerMode.toUpperCase());
            for (Condition condition : groupConditionsInfo.getConditions()) {
                if (condition == null) {
                    return badRequest(resp, "GroupConditionsInfo must have non null conditions: " + groupConditionsInfo);
                }
                condition.setTriggerId(groupId);
                if (condition.getTriggerMode() == null || !condition.getTriggerMode().equals(mode)) {
                    return badRequest(resp, "Condition: " + condition + " has a different triggerMode [" + triggerMode + "]");
                }
            }
            updatedConditions = definitionsService.setGroupConditions(tenantId, groupId, mode,
                    groupConditionsInfo.getConditions(),
                    groupConditionsInfo.getDataIdMemberMap());
            log.debugf("Conditions: %s", updatedConditions);
            return ok(resp, updatedConditions);
        } catch (NotFoundException e) {
            return notFound(resp, e.getMessage());
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> setTriggersEnabled(HttpServerResponse resp, String tenantId, Map<String, List<String>> params, boolean isGroupTrigger) {
        try {
            String triggerIds = null;
            Boolean enabled = null;
            if (params.get(PARAM_TRIGGER_IDS) != null) {
                triggerIds = params.get(PARAM_TRIGGER_IDS).get(0);
            }
            if (params.get(PARAM_ENABLED) != null) {
                enabled = Boolean.valueOf(params.get(PARAM_ENABLED).get(0));
            }
            if (isEmpty(triggerIds)) {
                return badRequest(resp, "TriggerIds must be non empty.");
            }
            if (null == enabled) {
                return badRequest(resp, "Enabled must be non-empty.");
            }
            definitionsService.updateTriggerEnablement(tenantId, triggerIds, enabled);
            return ok(resp);
        } catch (NotFoundException e) {
            return notFound(resp, e.getMessage());
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    TriggersCriteria buildCriteria(Map<String, List<String>> params) {
        String triggerIds = null;
        String tags = null;
        boolean thin = false;

        if (params.get(PARAM_TRIGGER_IDS) != null) {
            triggerIds = params.get(PARAM_TRIGGER_IDS).get(0);
        }
        if (params.get(PARAM_TAGS) != null) {
            tags = params.get(PARAM_TAGS).get(0);
        }
        if (params.get(PARAM_THIN) != null) {
            thin = Boolean.valueOf(params.get(PARAM_THIN).get(0));
        }

        TriggersCriteria criteria = new TriggersCriteria();
        if (!isEmpty(triggerIds)) {
            criteria.setTriggerIds(Arrays.asList(triggerIds.split(",")));
        }
        if (!isEmpty(tags)) {
            String[] tagTokens = tags.split(",");
            Map<String, String> tagsMap = new HashMap<>(tagTokens.length);
            for (String tagToken : tagTokens) {
                String[] fields = tagToken.split("\\|");
                if (fields.length == 2) {
                    tagsMap.put(fields[0], fields[1]);
                } else {
                    if (log.isDebugEnabled()) {
                        log.debugf("Invalid Tag Criteria %s", Arrays.toString(fields));
                    }
                    throw new IllegalArgumentException( "Invalid Tag Criteria " + Arrays.toString(fields) );
                }
            }
            criteria.setTags(tagsMap);
        }
        criteria.setThin(thin);

        return criteria;
    }
}
