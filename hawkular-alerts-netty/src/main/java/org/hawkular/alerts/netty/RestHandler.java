package org.hawkular.alerts.netty;

import org.reactivestreams.Publisher;

import reactor.ipc.netty.http.server.HttpServerRequest;
import reactor.ipc.netty.http.server.HttpServerResponse;

/**
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
public interface RestHandler {

    Publisher<Void> process(HttpServerRequest req, HttpServerResponse resp);
}
