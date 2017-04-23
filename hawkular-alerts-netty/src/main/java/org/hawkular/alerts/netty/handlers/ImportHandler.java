package org.hawkular.alerts.netty.handlers;

import static io.netty.handler.codec.http.HttpMethod.POST;
import static org.hawkular.alerts.api.json.JsonUtil.fromJson;
import static org.hawkular.alerts.netty.HandlersManager.TENANT_HEADER_NAME;
import static org.hawkular.alerts.netty.util.ResponseUtil.badRequest;
import static org.hawkular.alerts.netty.util.ResponseUtil.internalServerError;
import static org.hawkular.alerts.netty.util.ResponseUtil.isEmpty;
import static org.hawkular.alerts.netty.util.ResponseUtil.ok;

import java.util.List;
import java.util.Map;

import org.hawkular.alerts.api.model.export.Definitions;
import org.hawkular.alerts.api.model.export.ImportType;
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
@RestEndpoint(path = "/import")
public class ImportHandler implements RestHandler {
    private static final MsgLogger log = Logger.getMessageLogger(MsgLogger.class, ImportHandler.class.getName());
    private static final String ROOT = "/";

    DefinitionsService definitionsService;

    public ImportHandler() {
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
        // POST /{strategy}
        String[] tokens = subpath.substring(1).split(ROOT);
        if (method == POST && tokens.length == 1) {
            Definitions definitions;
            String json = req
                    .receive()
                    .aggregate()
                    .asString()
                    .block();
            try {
                definitions = fromJson(json, Definitions.class);
            } catch (Exception e) {
                log.errorf(e, "Error parsing Definitions json: %s. Reason: %s", json, e.toString());
                return badRequest(resp, e.toString());
            }
            return importDefinitions(resp, tenantId, tokens[0], definitions);
        }
        return badRequest(resp, "Wrong path " + method + " " + subpath);
    }

    Publisher<Void> importDefinitions(HttpServerResponse resp, String tenantId, String strategy, Definitions definitions) {
        try {
            ImportType importType = ImportType.valueOf(strategy.toUpperCase());
            Definitions imported = definitionsService.importDefinitions(tenantId, definitions, importType);
            return ok(resp, imported);
        } catch (IllegalArgumentException e) {
            return badRequest(resp, e.toString());
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }
}
