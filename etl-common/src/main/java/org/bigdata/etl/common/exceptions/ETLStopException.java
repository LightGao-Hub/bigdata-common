package org.bigdata.etl.common.exceptions;

/**
 * Author: GL
 * Date: 2022-05-11
 */
public class ETLStopException extends Exception {
    public ETLStopException(String message) {
        super(message);
    }
    public ETLStopException(String message, Throwable cause) {
        super(message, cause);
    }
}
