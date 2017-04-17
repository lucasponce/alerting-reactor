package org.hawkular.alerts.netty.handlers;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hawkular.alerts.api.json.JsonUtil.toJson;
import static org.hawkular.alerts.netty.HandlersManager.TENANT_HEADER_NAME;
import static org.hawkular.alerts.netty.util.ResponseUtil.isEmpty;
import static reactor.core.publisher.Mono.just;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hawkular.alerts.api.services.DefinitionsService;
import org.hawkular.alerts.engine.StandaloneAlerts;
import org.hawkular.alerts.log.MsgLogger;
import org.hawkular.alerts.netty.RestEndpoint;
import org.hawkular.alerts.netty.RestHandler;
import org.hawkular.alerts.netty.util.ResponseUtil.ApiError;
import org.jboss.logging.Logger;
import org.reactivestreams.Publisher;

import com.sun.net.httpserver.HttpServer;
import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.server.HttpServerResponse;

/**
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
@RestEndpoint(path = "/plugins")
public class ActionPluginHandler implements RestHandler {
    private static final MsgLogger log = Logger.getMessageLogger(MsgLogger.class, ActionPluginHandler.class.getName());

    DefinitionsService definitionsService;

    public ActionPluginHandler() {
        definitionsService = StandaloneAlerts.getDefinitionsService();
    }

    @Override
    public Publisher<Void> process(HttpServerRequest req,
                                   HttpServerResponse resp,
                                   String tenant,
                                   String subpath,
                                   Map<String, List<String>> params) {
        // @Path("/")
        if (subpath.equals("/")) {
            return findActionPlugins(resp);
        }
        // @Path("/{actionPlugin}")
        if (subpath.indexOf('/', 1) == -1) {
            String actionPlugin = subpath.substring(1);
            return getActionPlugin(resp, actionPlugin);
        }
        return resp
                .status(BAD_REQUEST)
                .sendString(just(toJson(new ApiError("Wrong path " + subpath))));
    }

    Publisher<Void> findActionPlugins(HttpServerResponse resp) {
        try {
            Collection<String> actionPlugins = definitionsService.getActionPlugins();
            log.debugf("ActionPlugins: %s", actionPlugins);
            return resp
                    .status(OK)
                    .sendString(just(toJson(actionPlugins)));
        } catch (Exception e) {
            log.errorf(e, "Error querying all plugins. Reason: %s", e.toString());
            return resp
                    .status(INTERNAL_SERVER_ERROR)
                    .sendString(just(toJson(new ApiError(e.toString()))));
        }
    }

    Publisher<Void> getActionPlugin(HttpServerResponse resp, String actionPlugin) {
        try {
            Set<String> actionPluginProps = definitionsService.getActionPlugin(actionPlugin);
            log.debugf("ActionPlugin: %s - Properties: %s", actionPlugin, actionPluginProps);
            if (actionPluginProps == null) {
                return resp
                        .status(NOT_FOUND)
                        .send();
            }
            return resp
                    .status(OK)
                    .sendString(just(toJson(actionPluginProps)));
        } catch (Exception e) {
            log.errorf(e, "Error querying plugin %s. Reason: %s", actionPlugin, e.toString());
            return resp
                    .status(INTERNAL_SERVER_ERROR)
                    .sendString(just(toJson(new ApiError(e.toString()))));
        }
    }
}
