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
        final ETLContext<SparkContext, RDD<String>> etlContext = new ETLContext<>(SparkETLTest.class, sc, jsonStr);
        etlContext.start();
    }

    @Test
    public void stop() throws Exception {
        final ETLContext<SparkContext, RDD<String>> etlContext = new ETLContext<>(SparkETLTest.class, sc, jsonStr);
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


    @Test
    public void earlySparkETL() throws IOException {
        final SourceExecutor<SparkContext, RDD<String>, FileConfig> source = new FileSourceExecutor();
        final MiddleExecutor<SparkContext, RDD<String>, DirtyConfig> middle = new DirtyMiddleExecutor();
        final SinkExecutor<SparkContext, RDD<String>, FileConfig> sink = new FileSinkExecutor();

        final String inputPath = Objects.requireNonNull(SparkETLTest.class.getClassLoader().getResource("input.txt")).getPath();
        final String outPutPath = new File(inputPath).getParent().concat("/out");
        FileUtils.deleteDirectory(new File(outPutPath));

        final FileConfig hdfsSourceConfig = new FileConfig(inputPath);
        final DirtyConfig dirtyConfig = new DirtyConfig();
        final FileConfig hdfsSinkConfig = new FileConfig(outPutPath);

        source.init(sc, hdfsSourceConfig);
        middle.init(sc, dirtyConfig);
        sink.init(sc, hdfsSinkConfig);

        Collection<RDD<String>> sourceProcess = source.process(sc, hdfsSourceConfig);
        Collection<RDD<String>> dirtyProcess = middle.process(sourceProcess, dirtyConfig);
        sink.process(dirtyProcess, hdfsSinkConfig);
    }

    @Test
    public void ealrySparkETL2() throws IOException, NoSuchMethodException, InvocationTargetException,
            IllegalAccessException {
        // 下面三种执行类在ETLContext中已经创建完毕，这里模拟的是反射执行etl过程.
        final SourceExecutor<SparkContext, RDD<String>, FileConfig> source = new FileSourceExecutor();
        final MiddleExecutor<SparkContext, RDD<String>, DirtyConfig> middle = new DirtyMiddleExecutor();
        final SinkExecutor<SparkContext, RDD<String>, FileConfig> sink = new FileSinkExecutor();

        final String inputPath = Objects.requireNonNull(SparkETLTest.class.getClassLoader().getResource("input.txt")).getPath();
        final String outPutPath = new File(inputPath).getParent().concat("/out");
        FileUtils.deleteDirectory(new File(outPutPath));

        final FileConfig hdfsSourceConfig = new FileConfig(inputPath);
        final DirtyConfig dirtyConfig = new DirtyConfig();
        final FileConfig hdfsSinkConfig = new FileConfig(outPutPath);

        // 初始化
        final Class<? extends SourceExecutor> sourceClass = source.getClass();
        final Method init = sourceClass.getMethod("init", sc.getClass(), FileConfig.class);
        init.invoke(source, sc, hdfsSourceConfig);

        final Class<? extends MiddleExecutor> middleClass = middle.getClass();
        final Method init2 = middleClass.getMethod("init", sc.getClass(), DirtyConfig.class);
        init2.invoke(middle, sc, dirtyConfig);

        final Class<? extends SinkExecutor> sinkClass = sink.getClass();
        final Method init3 = sinkClass.getMethod("init", sc.getClass(), FileConfig.class);
        init3.invoke(sink, sc, hdfsSinkConfig);

        // 执行
        final Method process = sourceClass.getMethod("process", sc.getClass(), FileConfig.class);
        final Object result1 = process.invoke(source, sc, hdfsSourceConfig);

        final Method process2 = middleClass.getMethod("process", Collection.class, DirtyConfig.class);
        final Object result2 = process2.invoke(middle, result1, dirtyConfig);

        final Method process3 = sinkClass.getMethod("process", Collection.class, FileConfig.class);
        process3.invoke(sink, result2, hdfsSinkConfig);
    }

}

