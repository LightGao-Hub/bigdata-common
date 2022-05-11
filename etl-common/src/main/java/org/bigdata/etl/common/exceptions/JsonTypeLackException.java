package org.bigdata.etl.common.exceptions;

/**
 * Author: GL
 * Date: 2022-05-11
 */
public class JsonTypeLackException extends RuntimeException {
    public JsonTypeLackException(String message) {
        super(message);
    }
    public JsonTypeLackException(String message, Throwable cause) {
        super(message, cause);
    }
}
