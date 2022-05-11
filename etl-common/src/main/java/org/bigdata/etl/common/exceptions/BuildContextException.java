package org.bigdata.etl.common.exceptions;

/**
 * Author: GL
 * Date: 2022-05-11
 */
public class BuildContextException extends Exception {
    public BuildContextException(String message) {
        super(message);
    }
    public BuildContextException(String message, Throwable cause) {
        super(message, cause);
    }
}
