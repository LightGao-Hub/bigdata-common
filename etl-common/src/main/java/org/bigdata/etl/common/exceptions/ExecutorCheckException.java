package org.bigdata.etl.common.exceptions;

/**
 * Author: GL
 * Date: 2022-05-11
 */
public class ExecutorCheckException extends RuntimeException {
    public ExecutorCheckException(String message) {
        super(message);
    }
    public ExecutorCheckException(String message, Throwable cause) {
        super(message, cause);
    }
}
