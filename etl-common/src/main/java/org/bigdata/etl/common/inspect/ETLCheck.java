package org.bigdata.etl.common.inspect;

import java.io.Serializable;

/**
 *  配置校验接口
 *
 * Author: GL
 * Date: 2022-04-22
 */
public interface ETLCheck<C extends Serializable> {
    boolean check(C config);
}
