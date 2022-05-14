package org.bigdata.etl.common.java.test.executors.transform;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.bigdata.etl.common.annotations.ETLExecutor;
import org.bigdata.etl.common.java.test.configs.DirtyConfig;
import org.bigdata.etl.common.executors.TransformExecutor;


/**
 * Author: GL
 * Date: 2022-04-22
 */
@Slf4j
@ETLExecutor("dirty")
public class DirtyTransformExecutor implements TransformExecutor<SparkContext, RDD<String>, RDD<String>, DirtyConfig> {

    @Override
    public void init(SparkContext engine, DirtyConfig config) {
        log.info("DirtyTransform init, config: {}", config);
    }

    @Override
    public RDD<String> process(SparkContext engine, RDD<String> value, DirtyConfig config) {
        log.info("DirtyTransform process, config: {}, Transform collect: {}", config, value.collect());
        return value;
    }

    @Override
    public void close(SparkContext engine, DirtyConfig config) {
        log.info("DirtyTransformExecutor close, config: {}", config);
    }

    @Override
    public boolean check(DirtyConfig config) {
        log.info("DirtyTransformExecutor check, config: {}", config);
        return true;
    }
}
