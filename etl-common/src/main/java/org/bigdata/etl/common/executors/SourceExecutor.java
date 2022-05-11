package org.bigdata.etl.common.executors;

import java.io.Serializable;

import org.bigdata.etl.common.inspect.ETLCheck;

/**
 *  执行器接口
 *  E: engine-计算框架引擎类, 例如SparkContext / FLink-env
 *  O: data-计算框架内部的执行类型, 例如spark的DataFrame / flink的DataStream
 *  C: config-为此执行器的配置类
 *
 * Author: GL
 * Date: 2022-04-21
 */
public interface SourceExecutor<E, O, C extends Serializable> extends ETLCheck<C>, Serializable {

    void init(E engine, C config);

    O process(E engine, C config);

    void close(E engine, C config);
}
