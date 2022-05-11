package org.bigdata.etl.common.exceptions;

/**
 * Author: GL
 * Date: 2022-05-11
 */
public class AnnotationInterfaceMismatchException extends RuntimeException {
    public AnnotationInterfaceMismatchException(String message) {
        super(message);
    }
    public AnnotationInterfaceMismatchException(String message, Throwable cause) {
        super(message, cause);
    }
}
