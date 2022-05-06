package org.bigdata.etl.common.executors.sink;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.bigdata.etl.common.annotations.ETLExecutor;
import org.bigdata.etl.common.configs.FileConfig;
import org.bigdata.etl.common.executors.SinkExecutor;

import java.util.Collection;
import java.util.Optional;


/**
 * Author: GL
 * Date: 2022-04-22
 */
@Slf4j
@ETLExecutor("file")
public class FileSinkExecutor implements SinkExecutor<SparkContext, RDD<String>, FileConfig> {

    @Override
    public void init(SparkContext engine, FileConfig config) {
        log.info("FileSinkExecutor init, config: {}", config);
    }

    @Override
    public void process(SparkContext engine, RDD<String> value, FileConfig config) {
        log.info("FileSinkExecutor process, config: {}", config);
        value.saveAsTextFile(config.getPath());
    }

    @Override
    public void close(SparkContext engine, FileConfig config) {
        log.info("FileSinkExecutorExecutor close, config: {}", config);
    }

    @Override
    public boolean check(FileConfig config) {
        log.info("FileSinkExecutorExecutor check, config: {}", config);
        return true;
    }
}
