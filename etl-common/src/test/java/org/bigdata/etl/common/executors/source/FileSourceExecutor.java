package org.bigdata.etl.common.executors.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.bigdata.etl.common.annotations.ETLExecutor;
import org.bigdata.etl.common.configs.FileConfig;
import org.bigdata.etl.common.executors.SourceExecutor;

import java.util.Collection;
import java.util.Collections;


/**
 * Author: GL
 * Date: 2022-04-22
 */
@Slf4j
@ETLExecutor("file")
public class FileSourceExecutor implements SourceExecutor<SparkContext, RDD<String>, FileConfig> {

    @Override
    public void init(SparkContext engine, FileConfig config) {
        log.info("FileSourceExecutor init, config: {}", config);
    }

    @Override
    public Collection<RDD<String>> process(SparkContext engine, FileConfig config) {
        log.info("FileSourceExecutor process, config: {}", config);
        final RDD<String> stringRDD = engine.textFile(config.getPath(), 2);
        return Collections.singleton(stringRDD);
    }


    @Override
    public void close(SparkContext engine, FileConfig config) {
        log.info("FileSourceExecutor close, config: {}", config);
    }

    @Override
    public boolean check(FileConfig config) {
        log.info("FileSourceExecutor check, config: {}", config.getPath());
        return true;
    }
}
