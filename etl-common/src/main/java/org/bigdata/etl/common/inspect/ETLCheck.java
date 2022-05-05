package org.bigdata.etl.common.inspect;

import org.bigdata.etl.common.configs.ExecutorConfig;

/**
 *  配置校验接口
 *
 * Author: GL
 * Date: 2022-04-22
 */
public interface ETLCheck<C extends ExecutorConfig> {
    boolean check(C config);
}
