package org.bigdata.etl.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.bigdata.etl.common.configs.DirtyConfig;
import org.bigdata.etl.common.configs.FileConfig;
import org.bigdata.etl.common.context.ETLContext;
import org.bigdata.etl.common.executors.MiddleExecutor;
import org.bigdata.etl.common.executors.SinkExecutor;
import org.bigdata.etl.common.executors.SourceExecutor;
import org.bigdata.etl.common.executors.source.*;
import org.bigdata.etl.common.executors.middle.*;
import org.bigdata.etl.common.executors.sink.*;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;


/**
 * Author: GL
 * Date: 2022-04-24
 */
@Slf4j
public class SparkETLTest {

    private SparkContext sc;
    private final String inputPath = Objects.requireNonNull(SparkETLTest.class.getClassLoader().getResource("input.txt")).getPath();
    private final String outPutPath = new File(inputPath).getParent().concat("/out");
    private final String jsonStr = "{\"source\":{\"processType\":\"file\",\"path\":\""+inputPath+"\"},"
            + "\"middle\":[{\"processType\":\"dirty\",\"dirtyPath\":\"/dirty\"},{\"processType\":\"schema\","
            + "\"dirtyPath\":\"/dirty2\"}],\"sink\":[{\"processType\":\"file\",\"path\":\""+outPutPath+"\"}]}";

    @Before
    public void init() throws IOException {
        SparkConf conf = new SparkConf().setAppName("test etl").setMaster("local[2]");
        sc = new SparkContext(conf);
        FileUtils.deleteDirectory(new File(outPutPath));
    }

    @Test
    public void start() throws Exception {
        final ETLContext<SparkContext> etlContext = new ETLContext<>(SparkETLTest.class, sc, jsonStr);
        etlContext.start();
    }

    @Test
    public void stop() throws Exception {
        final ETLContext<SparkContext> etlContext = new ETLContext<>(SparkETLTest.class, sc, jsonStr);
        etlContext.close();
    }

    @Test
    public void sparkDemo() {
        final RDD<String> stringRDD = sc.textFile(inputPath, 2);
        final Object collect = stringRDD.collect();
        log.info("time: {}, collect: {}", System.currentTimeMillis(), collect);
        final Object collect2 = stringRDD.collect();
        log.info("time: {}, collect: {}", System.currentTimeMillis(), collect2);
        final Object collect3 = stringRDD.collect();
        log.info("time: {}, collect: {}", System.currentTimeMillis(), collect3);
        final Object collect4 = stringRDD.collect();
        log.info("time: {}, collect: {}", System.currentTimeMillis(), collect4);
    }

}

