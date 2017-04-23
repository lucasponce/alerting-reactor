package org.hawkular.alerts.netty.handlers;

import static io.netty.handler.codec.http.HttpMethod.DELETE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.POST;
import static io.netty.handler.codec.http.HttpMethod.PUT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hawkular.alerts.api.json.JsonUtil.collectionFromJson;
import static org.hawkular.alerts.api.json.JsonUtil.fromJson;
import static org.hawkular.alerts.api.json.JsonUtil.toJson;
import static org.hawkular.alerts.netty.HandlersManager.TENANT_HEADER_NAME;
import static org.hawkular.alerts.netty.util.ResponseUtil.badRequest;
import static org.hawkular.alerts.netty.util.ResponseUtil.checkTags;
import static org.hawkular.alerts.netty.util.ResponseUtil.extractPaging;
import static org.hawkular.alerts.netty.util.ResponseUtil.internalServerError;
import static org.hawkular.alerts.netty.util.ResponseUtil.isEmpty;
import static org.hawkular.alerts.netty.util.ResponseUtil.notFound;
import static org.hawkular.alerts.netty.util.ResponseUtil.ok;
import static org.hawkular.alerts.netty.util.ResponseUtil.paginatedOk;
import static org.hawkular.alerts.netty.util.ResponseUtil.parseTagQuery;
import static org.hawkular.alerts.netty.util.ResponseUtil.parseTags;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hawkular.alerts.api.model.event.Event;
import org.hawkular.alerts.api.model.paging.Page;
import org.hawkular.alerts.api.model.paging.Pager;
import org.hawkular.alerts.api.services.AlertsService;
import org.hawkular.alerts.api.services.EventsCriteria;
import org.hawkular.alerts.engine.StandaloneAlerts;
import org.hawkular.alerts.log.MsgLogger;
import org.hawkular.alerts.netty.RestEndpoint;
import org.hawkular.alerts.netty.RestHandler;
import org.hawkular.alerts.netty.handlers.EventsWatcher.EventsListener;
import org.jboss.logging.Logger;
import org.reactivestreams.Publisher;

import io.netty.handler.codec.http.HttpMethod;
import reactor.core.publisher.Flux;
import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.server.HttpServerResponse;

/**
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
@RestEndpoint(path = "/events")
public class EventsHandler implements RestHandler {
    private static final MsgLogger log = Logger.getMessageLogger(MsgLogger.class, EventsHandler.class.getName());
    private static final String ROOT = "/";
    private static final String DATA = "/data";
    private static final String TAGS = "/tags";
    private static final String WATCH = "/watch";
    private static final String _DELETE = "/delete";
    private static final String EVENT = "/event";
    private static final String PARAM_START_TIME = "startTime";
    private static final String PARAM_END_TIME = "endTime";
    private static final String PARAM_EVENT_IDS = "eventIds";
    private static final String PARAM_TRIGGER_IDS = "triggerIds";
    private static final String PARAM_CATEGORIES = "categories";
    private static final String PARAM_TAGS = "tags";
    private static final String PARAM_TAG_QUERY = "tagQuery";
    private static final String PARAM_THIN = "thin";
    private static final String PARAM_WATCH_INTERVAL = "watchInterval";
    private static final String PARAM_TAG_NAMES = "tagNames";

    AlertsService alertsService;

    public EventsHandler() {
        alertsService = StandaloneAlerts.getAlertsService();
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

        // POST /
        if (method == POST && subpath.equals(ROOT)) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            Event event;
            try {
                event = fromJson(json, Event.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing Event json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return createEvent(resp, tenantId, event);
        }
        // POST /data
        if (method == POST && subpath.equals(DATA)) {
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            Collection<Event> events;
            try {
                events = collectionFromJson(json, Event.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing Event json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return sendEvents(resp, tenantId, events);
        }
        // PUT /tags
        if (method == PUT && subpath.equals(TAGS)) {
            return addTags(resp, tenantId, params);
        }
        // DELETE /tags
        if (method == DELETE && subpath.equals(TAGS)) {
            return removeTags(resp, tenantId, params);
        }
        // GET /
        if (method == GET && subpath.equals(ROOT)) {
            return findEvents(req, resp, tenantId, params, req.uri());
        }
        // GET /watch
        if (method == GET && subpath.equals(WATCH)) {
            return watchEvents(resp, tenantId, params);
        }
        // PUT /delete
        if (method == PUT && subpath.equals(_DELETE)) {
            return deleteEvents(resp, tenantId, params);
        }
        String[] tokens = subpath.substring(1).split(ROOT);
        // GET /event/{eventId}
        if (method == GET && subpath.startsWith(EVENT) && tokens.length == 2) {
            return getEvent(resp, tenantId, tokens[1], params);
        }

        return badRequest(resp, "Wrong path " + method + " " + subpath);
    }

    Publisher<Void> createEvent(HttpServerResponse resp, String tenantId, Event event) {
        try {
            if (event != null) {
                if (isEmpty(event.getId())) {
                    return badRequest(resp, "Event with id null.");
                }
                if (isEmpty(event.getCategory())) {
                    return badRequest(resp, "Event with category null.");
                }
                event.setTenantId(tenantId);
                if (null != alertsService.getEvent(tenantId, event.getId(), true)) {
                    return badRequest(resp, "Event with ID [" + event.getId() + "] exists.");
                }
                if (!checkTags(event)) {
                    return badRequest(resp, "Tags " + event.getTags() + " must be non empty.");
                }
                alertsService.addEvents(Collections.singletonList(event));
                return ok(resp, event);
            }
            return badRequest(resp, "Event is null");
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> sendEvents(HttpServerResponse resp, String tenantId, Collection<Event> events) {
        try {
            if (isEmpty(events)) {
                return badRequest(resp, "Events is empty");
            }
            events.stream().forEach(ev -> ev.setTenantId(tenantId));
            alertsService.sendEvents(events);
            log.debugf("Events: ", events);
            return ok(resp);
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> addTags(HttpServerResponse resp, String tenantId, Map<String, List<String>> params) {
        String eventIds = null;
        String tags = null;
        if (params.get(PARAM_EVENT_IDS) != null) {
            eventIds = params.get(PARAM_EVENT_IDS).get(0);
        }
        if (params.get(PARAM_TAGS) != null) {
            tags = params.get(PARAM_TAGS).get(0);
        }
        try {
            if (!isEmpty(eventIds) && !isEmpty(tags)) {
                List<String> eventIdList = Arrays.asList(eventIds.split(","));
                Map<String, String> tagsMap = parseTags(tags);
                alertsService.addEventTags(tenantId, eventIdList, tagsMap);
                log.debugf("Tagged eventIds:%s, %s", eventIdList, tagsMap);
                return ok(resp);
            }
            return badRequest(resp, "EventIds and Tags required for adding tags");
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> removeTags(HttpServerResponse resp, String tenantId, Map<String, List<String>> params) {
        String eventIds = null;
        String tagNames = null;
        if (params.get(PARAM_EVENT_IDS) != null) {
            eventIds = params.get(PARAM_EVENT_IDS).get(0);
        }
        if (params.get(PARAM_TAG_NAMES) != null) {
            tagNames = params.get(PARAM_TAG_NAMES).get(0);
        }
        try {
            if (!isEmpty(eventIds) && !isEmpty(tagNames)) {
                Collection<String> ids = Arrays.asList(eventIds.split(","));
                Collection<String> tags = Arrays.asList(tagNames.split(","));
                alertsService.removeEventTags(tenantId, ids, tags);
                log.debugf("Untagged eventsIds:%s, %s", ids, tags);
                return ok(resp);
            }
            return badRequest(resp, "EventIds and Tags required for removing tags");
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> findEvents(HttpServerRequest req, HttpServerResponse resp, String tenantId, Map<String, List<String>> params, String uri) {
        try {
            Pager pager = extractPaging(params);
            EventsCriteria criteria = buildCriteria(params);
            Page<Event> eventPage = alertsService.getEvents(tenantId, criteria, pager);
            log.debugf("Events: %s", eventPage);
            if (isEmpty(eventPage)) {
                return ok(resp, eventPage);
            }
            return paginatedOk(req, resp, eventPage, uri);
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> watchEvents(HttpServerResponse resp, String tenantId, Map<String, List<String>> params) {
        EventsCriteria criteria = buildCriteria(params);
        Flux<String> watcherFlux = Flux.create(sink -> {
            Long watchInterval = null;
            if (params.get(PARAM_WATCH_INTERVAL) != null) {
                watchInterval = Long.valueOf(params.get(PARAM_WATCH_INTERVAL).get(0));
            }
            EventsListener listener = event -> {
                sink.next(toJson(event) + "\r\n");
            };
            String channelId = resp.context().channel().id().asShortText();
            EventsWatcher watcher = new EventsWatcher(channelId, listener, Collections.singleton(tenantId), criteria, watchInterval);
            sink.onCancel(() -> watcher.dispose());
            watcher.start();
        });
        resp.status(OK);
        // Watcher send events one by one, so flux is splited in windows of one element
        return watcherFlux.window(1).concatMap(w -> resp.sendString(w));
    }

    Publisher<Void> deleteEvents(HttpServerResponse resp, String tenantId, Map<String, List<String>> params) {
        try {
            EventsCriteria criteria = buildCriteria(params);
            int numDeleted = alertsService.deleteEvents(tenantId, criteria);
            log.debugf("Events deleted: ", numDeleted);
            Map<String, String> deleted = new HashMap<>();
            deleted.put("deleted", String.valueOf(numDeleted));
            return ok(resp, deleted);
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    Publisher<Void> getEvent(HttpServerResponse resp, String tenantId, String eventId, Map<String, List<String>> params) {
        try {
            boolean thin = false;
            if (params.get(PARAM_THIN) != null) {
                thin = Boolean.valueOf(params.get(PARAM_THIN).get(0));
            }
            Event found = alertsService.getEvent(tenantId, eventId, thin);
            if (found != null) {
                log.debugf("Event: ", found);
                return ok(resp, found);
            }
            return notFound(resp, "eventId: " + eventId + " not found");
        } catch (IllegalArgumentException e) {
            return badRequest(resp,"Bad arguments: " + e.getMessage());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }

    EventsCriteria buildCriteria(Map<String, List<String>> params) {
        Long startTime = null;
        Long endTime = null;
        String eventIds = null;
        String triggerIds = null;
        String categories = null;
        String tags = null;
        String tagQuery = null;
        boolean thin = false;

        if (params.get(PARAM_START_TIME) != null) {
            startTime = Long.valueOf(params.get(PARAM_START_TIME).get(0));
        }
        if (params.get(PARAM_END_TIME) != null) {
            endTime = Long.valueOf(params.get(PARAM_END_TIME).get(0));
        }
        if (params.get(PARAM_EVENT_IDS) != null) {
            eventIds = params.get(PARAM_EVENT_IDS).get(0);
        }
        if (params.get(PARAM_TRIGGER_IDS) != null) {
            triggerIds = params.get(PARAM_TRIGGER_IDS).get(0);
        }
        if (params.get(PARAM_CATEGORIES) != null) {
            categories = params.get(PARAM_CATEGORIES).get(0);
        }
        if (params.get(PARAM_TAGS) != null) {
            tags = params.get(PARAM_TAGS).get(0);
        }
        if (params.get(PARAM_TAG_QUERY) != null) {
            tagQuery = params.get(PARAM_TAG_QUERY).get(0);
        }
        String unifiedTagQuery;
        if (!isEmpty(tags)) {
            unifiedTagQuery = parseTagQuery(parseTags(tags));
        } else {
            unifiedTagQuery = tagQuery;
        }
        if (params.get(PARAM_THIN) != null) {
            thin = Boolean.valueOf(params.get(PARAM_THIN).get(0));
        }
        return new EventsCriteria(startTime, endTime, eventIds, triggerIds, categories, unifiedTagQuery, thin);
    }
}
