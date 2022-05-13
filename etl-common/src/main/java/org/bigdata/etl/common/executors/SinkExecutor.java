package org.bigdata.etl.common.executors;

import java.io.Serializable;

import org.bigdata.etl.common.inspect.ETLCheck;

/**
 *  执行器接口
 *  E: engine-计算框架引擎类, 例如SparkContext / FLink-env
 *  D: 最后一个TransformExecutor的输出类型
 *  C: config-为此执行器的配置类
 *
 * Author: GL
 * Date: 2022-04-21
 */
public interface SinkExecutor<E, I, C extends Serializable> extends ETLCheck<C>, Serializable {

    void init(E engine, C config); // 泛型参数及jsonObject的值

    void process(E engine, I value, C config);

    void close(E engine, C config);
}
