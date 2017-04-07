package org.hawkular.alerting.reactor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.reactivestreams.Publisher;

import io.netty.handler.codec.http.QueryStringDecoder;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.server.HttpServerResponse;

/**
 * Define logic of Rest Handlers
 */
public class RestHandlers {
    private static final Logger log = LogManager.getLogger(RestHandlers.class);
    private static final String BASE = "/hawkular/alerts";

    public Publisher<Void> process(HttpServerRequest req, HttpServerResponse resp) {
        QueryStringDecoder query = new QueryStringDecoder(req.uri());
        log.info("route: {} {}\n", query.path(), query.parameters());
        String base = query.path().substring(0, BASE.length());
        if (BASE.equals(base)) {
            String endpoint = query.path().substring(BASE.length());
            switch (req.method().name()) {
                case "POST":
                    if (endpoint.startsWith("/data")) {
                        return sendData(req, resp);
                    }
                    break;
                case "PUT":
                    if (endpoint.startsWith("/triggers")) {

                    }
                case "GET":
                    if (query.path().startsWith("")) {
                        return getAlerts(req, resp);
                    }
                    break;
            }
        }
        return resp.sendString(Mono.just("Not supported"));
    }

    public Publisher<Void> sendData(HttpServerRequest req, HttpServerResponse resp) {
        return resp.sendString(Mono.just("Data received"));
    }

    public Publisher<Void> getAlerts(HttpServerRequest req, HttpServerResponse resp) {
        return resp.sendString(Mono.just("List alerts"));
    }
}
