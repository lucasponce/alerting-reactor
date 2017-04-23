package org.hawkular.alerts.netty.handlers;

import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static org.hawkular.alerts.api.json.JsonUtil.collectionFromJson;
import static org.hawkular.alerts.api.json.JsonUtil.fromJson;
import static org.hawkular.alerts.netty.HandlersManager.TENANT_HEADER_NAME;
import static org.hawkular.alerts.netty.util.ResponseUtil.badRequest;
import static org.hawkular.alerts.netty.util.ResponseUtil.isEmpty;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.hawkular.alerts.api.json.GroupConditionsInfo;
import org.hawkular.alerts.api.json.GroupMemberInfo;
import org.hawkular.alerts.api.json.UnorphanMemberInfo;
import org.hawkular.alerts.api.model.condition.Condition;
import org.hawkular.alerts.api.model.dampening.Dampening;
import org.hawkular.alerts.api.model.trigger.FullTrigger;
import org.hawkular.alerts.api.model.trigger.Mode;
import org.hawkular.alerts.api.model.trigger.Trigger;
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

    DefinitionsService definitions;

    public TriggersHandler() {
        definitions = StandaloneAlerts.getDefinitionsService();
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
        if (method == GET && tokens.length == 0) {
            return findTriggers(req, resp, tenantId, params, req.uri());
        }
        // POST /
        if (method == POST && tokens.length == 0) {
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
            return findGroupMembers(req, resp, tenantId, tokens[1], params, req.uri());
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
        return null;
    }

    Publisher<Void> createFullTrigger(HttpServerResponse resp, String tenantId, FullTrigger fullTrigger) {
        return null;
    }

    Publisher<Void> createGroupMember(HttpServerResponse resp, String tenantId, GroupMemberInfo groupMember) {
        return null;
    }

    Publisher<Void> createTrigger(HttpServerResponse resp, String tenantId, Trigger trigger, boolean isGroupTrigger) {
        return null;
    }

    Publisher<Void> deleteDampening(HttpServerResponse resp, String tenantId, String triggerId, String dampeningId, boolean isGroupTrigger) {
        return null;
    }

    Publisher<Void> deleteGroupTrigger(HttpServerResponse resp, String tenantId, String groupId, Map<String, List<String>> params) {
        return null;
    }

    Publisher<Void> deleteTrigger(HttpServerResponse resp, String tenantId, String triggerId) {
        return null;
    }

    Publisher<Void> findGroupMembers(HttpServerRequest req, HttpServerResponse resp, String tenantId, String groupId, Map<String, List<String>> params, String uri) {
        return null;
    }

    Publisher<Void> findTriggers(HttpServerRequest req, HttpServerResponse resp, String tenantId, Map<String, List<String>> params, String uri) {
        return null;
    }

    Publisher<Void> getDampening(HttpServerResponse resp, String tenantId, String triggerId, String dampeningId) {
        return null;
    }

    Publisher<Void> getTrigger(HttpServerResponse resp, String tenantId, String triggerId, boolean isFullTrigger) {
        return null;
    }

    Publisher<Void> getTriggerConditions(HttpServerResponse resp, String tenantId, String triggerId) {
        return null;
    }

    Publisher<Void> getTriggerDampenings(HttpServerResponse resp, String tenantId, String triggerId, Mode triggerMode) {
        return null;
    }

    Publisher<Void> updateTrigger(HttpServerResponse resp, String tenantId, String triggerId, Trigger trigger, boolean isGroupTrigger) {
        return null;
    }

    Publisher<Void> orphanMemberTrigger(HttpServerResponse resp, String tenantId, String memberId) {
        return null;
    }

    Publisher<Void> updateDampening(HttpServerResponse resp, String tenantId, String triggerId, String dampeningId, Dampening dampening, boolean isGroupTrigger) {
        return null;
    }

    Publisher<Void> unorphanMemberTrigger(HttpServerResponse resp, String tenantId, String memberId, UnorphanMemberInfo unorphanMemberInfo) {
        return null;
    }

    Publisher<Void> setConditions(HttpServerResponse resp, String tenantId, String triggerId, String triggerMode, Collection<Condition> conditions) {
        return null;
    }

    Publisher<Void> setGroupConditions(HttpServerResponse resp, String tenantId, String groupId, String triggerMode, GroupConditionsInfo groupConditionsInfo) {
        return null;
    }

    Publisher<Void> setTriggersEnabled(HttpServerResponse resp, String tenantId, Map<String, List<String>> params, boolean isGroupTrigger) {
        return null;
    }
}
