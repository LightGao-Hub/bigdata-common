package org.bigdata.etl.common.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * etl注解
 *
 * Author: GL
 * Date: 2022-04-22
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface ETLExecutor {

    /**
     * processType字符串
     */
    String value();

}
