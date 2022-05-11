package org.bigdata.etl.common.exceptions;

/**
 * Author: GL
 * Date: 2022-05-11
 */
public class ExecutorConfigNotFoundException extends RuntimeException {
    public ExecutorConfigNotFoundException(String message) {
        super(message);
    }
    public ExecutorConfigNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
