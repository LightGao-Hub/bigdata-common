package org.bigdata.etl.common.executors.transform;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.bigdata.etl.common.annotations.ETLExecutor;
import org.bigdata.etl.common.configs.NilExecutorConfig;
import org.bigdata.etl.common.executors.TransformExecutor;


/**
 * Author: GL
 * Date: 2022-04-22
 */
@Slf4j
@ETLExecutor("schema")
public class SchemaTransformExecutor implements TransformExecutor<SparkContext, RDD<String>, RDD<String>, NilExecutorConfig> {

    @Override
    public void init(SparkContext engine, NilExecutorConfig config) {
        log.info("SchemaTransformExecutor init, config: {}", config);
    }

    @Override
    public RDD<String> process(SparkContext engine, RDD<String> value, NilExecutorConfig config) {
        return value;
    }

    @Override
    public void close(SparkContext engine, NilExecutorConfig config) {
        log.info("SchemaTransformExecutor close, config: {}", config);
    }

    @Override
    public boolean check(NilExecutorConfig config) {
        log.info("SchemaTransformExecutor check, config: {}", config);
        return true;
    }
}
