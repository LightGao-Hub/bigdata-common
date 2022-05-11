package org.bigdata.etl.common.exceptions;

/**
 * Author: GL
 * Date: 2022-05-11
 */
public class ETLStartException extends Exception {
    public ETLStartException(String message) {
        super(message);
    }
    public ETLStartException(String message, Throwable cause) {
        super(message, cause);
    }
}
