package com.haizhi.sql.parser.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface LibraryOperatorEx {

    /**
     * The set of libraries that this function or operator belongs to.
     * Must not be null or empty.
     */
    SqlLibraryEx[] libraries();
}
