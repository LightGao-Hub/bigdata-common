package org.bigdata.etl.common.executors.middle;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.bigdata.etl.common.annotations.ETLExecutor;
import org.bigdata.etl.common.configs.DirtyConfig;
import org.bigdata.etl.common.configs.SchemaConfig;
import org.bigdata.etl.common.executors.MiddleExecutor;

import java.util.Collection;
import java.util.Optional;


/**
 * Author: GL
 * Date: 2022-04-22
 */
@Slf4j
@ETLExecutor("schema")
public class SchemaMiddleExecutor implements MiddleExecutor<SparkContext, RDD<String>, RDD<String>, SchemaConfig> {

    @Override
    public void init(SparkContext engine, SchemaConfig config) {
        log.info("SchemaMiddleExecutor init, config: {}", config);
    }

    @Override
    public RDD<String> process(SparkContext engine, RDD<String> value, SchemaConfig config) {
        return value;
    }

    @Override
    public void close(SparkContext engine, SchemaConfig config) {
        log.info("SchemaMiddleExecutor close, config: {}", config);
    }

    @Override
    public boolean check(SchemaConfig config) {
        log.info("SchemaMiddleExecutor check, config: {}", config);
        return true;
    }
}
