package org.hawkular.alerts.netty.handlers;

import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static org.hawkular.alerts.netty.HandlersManager.TENANT_HEADER_NAME;
import static org.hawkular.alerts.netty.util.ResponseUtil.badRequest;
import static org.hawkular.alerts.netty.util.ResponseUtil.isEmpty;

import java.util.List;
import java.util.Map;

import org.hawkular.alerts.log.MsgLogger;
import org.hawkular.alerts.netty.RestEndpoint;
import org.hawkular.alerts.netty.RestHandler;
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
@RestEndpoint(path = "/")
public class AlertsHandler implements RestHandler {
    private static final MsgLogger log = Logger.getMessageLogger(MsgLogger.class, AlertsHandler.class.getName());
    private static final String ROOT = "/";

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
            return testStreaming(resp);
        }
        return badRequest(resp, "Wrong path " + method + " " + subpath);
    }

    Publisher<Void> testStreaming(HttpServerResponse resp) {
        Flux<String> watcherFlux = Flux.create(sink -> {
            WatcherListener listener = message -> {
                sink.next(message + "\r\n");
            };
            Watcher watcher = new Watcher(resp.context().channel().id().asShortText(), listener);
            sink.onCancel(() -> watcher.setRunning(false));
            watcher.start();
        });
        resp.status(OK);
        return watcherFlux.window(1).concatMap(w -> resp.sendString(w));
    }

    public interface WatcherListener {
        void onMessage(String message);
    }

    public static class Watcher extends Thread {
        WatcherListener listener;
        String id;

        boolean running = true;

        public Watcher(String id, WatcherListener listener) {
            super("Watcher["+id+"]");
            this.id = id;
            this.listener = listener;
        }

        public void setRunning(boolean running) {
            this.running = running;
        }

        @Override
        public void run() {
            if (listener == null) {
                log.error("Listener is null");
                return;
            }
            int count = 0;
            while (running) {
                String message = "{\"status\":\"OPEN\",\"date\":\"" + new java.util.Date() + " - " + count++ + "\"}";
                log.info(message);
                listener.onMessage(message);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.error(e);
                }
            }
            log.info("Watcher [" + id + "] finished");
        }
    }
}
