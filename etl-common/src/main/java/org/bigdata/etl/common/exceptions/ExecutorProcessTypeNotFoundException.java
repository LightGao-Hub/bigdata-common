package org.bigdata.etl.common.exceptions;

/**
 * Author: GL
 * Date: 2022-05-11
 */
public class ExecutorProcessTypeNotFoundException extends RuntimeException {
    public ExecutorProcessTypeNotFoundException(String message) {
        super(message);
    }
    public ExecutorProcessTypeNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}
