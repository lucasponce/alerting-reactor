package org.hawkular.alerts.netty.handlers;

import static io.netty.handler.codec.http.HttpMethod.GET;
import static org.hawkular.alerts.netty.HandlersManager.TENANT_HEADER_NAME;
import static org.hawkular.alerts.netty.util.ResponseUtil.badRequest;
import static org.hawkular.alerts.netty.util.ResponseUtil.internalServerError;
import static org.hawkular.alerts.netty.util.ResponseUtil.isEmpty;
import static org.hawkular.alerts.netty.util.ResponseUtil.ok;

import java.util.List;
import java.util.Map;

import org.hawkular.alerts.api.model.export.Definitions;
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
@RestEndpoint(path = "/export")
public class ExportHandler implements RestHandler {
    private static final MsgLogger log = Logger.getMessageLogger(MsgLogger.class, ExportHandler.class.getName());
    private static final String ROOT = "/";

    DefinitionsService definitionsService;

    public ExportHandler() {
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
        if (method == GET && subpath.equals(ROOT)) {
            return exportDefinitions(resp, tenantId);
        }
        return badRequest(resp, "Wrong path " + method + " " + subpath);
    }

    Publisher<Void> exportDefinitions(HttpServerResponse resp, String tenantId) {
        try {
            Definitions definitions = definitionsService.exportDefinitions(tenantId);
            return ok(resp, definitions);
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            return internalServerError(resp, e.toString());
        }
    }
}
