package org.hawkular.alerts.netty.util;

import org.reactivestreams.Publisher;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * @author Jay Shaughnessy
 * @author Lucas Ponce
 */
public class ResponseUtil {

    public static class ApiError {
        @JsonInclude
        private final String errorMsg;

        public ApiError(String errorMsg) {
            this.errorMsg = errorMsg != null && !errorMsg.trim().isEmpty() ? errorMsg : "No details";
        }

        public String getErrorMsg() {
            return errorMsg;
        }
    }

    public static boolean isEmpty(String s) {
        return s == null || s.isEmpty();
    }
}
