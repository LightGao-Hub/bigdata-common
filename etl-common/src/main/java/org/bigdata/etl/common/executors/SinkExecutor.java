package org.bigdata.etl.common.executors;

import java.io.Serializable;
import java.util.Collection;

import org.bigdata.etl.common.configs.ExecutorConfig;
import org.bigdata.etl.common.inspect.ETLCheck;

/**
 *  执行器接口
 *  E: engine-计算框架引擎类, 例如SparkContext / FLink-env
 *  D: data-计算框架内部的执行类型, 例如spark的DataFrame / flink的DataStream
 *  C: config-为此执行器的配置类
 *
 * Author: GL
 * Date: 2022-04-21
 */
public interface SinkExecutor<E, I, C extends ExecutorConfig> extends ETLCheck<C>, Serializable {

    void init(E engine, C config); // 泛型参数及jsonObject的值

    void process(E engine, I value, C config);

    void close(E engine, C config);
}
