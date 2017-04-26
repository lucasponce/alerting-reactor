package org.hawkular.alerts.netty.handlers;

import static org.hawkular.alerts.netty.util.ResponseUtil.ok;

import java.util.List;
import java.util.Map;

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
@RestEndpoint(path = "/test")
public class TestHandler implements RestHandler {
    private static final MsgLogger log = Logger.getMessageLogger(MsgLogger.class, TestHandler.class.getName());

    public Publisher<Void> process(HttpServerRequest req,
                                   HttpServerResponse resp,
                                   String tenantId,
                                   String subpath,
                                   Map<String, List<String>> params) {
        log.info("TEST");
        HttpMethod method = req.method();
        String json = req
                .receive()
                .aggregate()
                .asString()
                .block();
        log.infof("TEST %s ", json);
        return ok(resp);
    }
}
