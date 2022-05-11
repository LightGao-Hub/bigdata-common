package org.bigdata.etl.common.exceptions;

/**
 * Author: GL
 * Date: 2022-05-11
 */
public class LeastSourceSinkException extends RuntimeException {
    public LeastSourceSinkException(String message) {
        super(message);
    }
    public LeastSourceSinkException(String message, Throwable cause) {
        super(message, cause);
    }
}
