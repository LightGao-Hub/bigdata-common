package org.bigdata.etl.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.bigdata.etl.common.context.ETLContext;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Objects;


/**
 * Author: GL
 * Date: 2022-04-24
 */
@Slf4j
public class SparkETLTest {

    private final String inputPath = Objects.requireNonNull(SparkETLTest.class.getClassLoader().getResource("input.txt")).getPath();
    private final String outPutPath = new File(inputPath).getParent().concat("/out");
    private final String dirtyPath = new File(inputPath).getParent().concat("/dirty");
    private final String jsonStr = String.format("{\"source\":{\"processType\":\"file\","
                    + "\"config\":{\"path\":\"%s\"}},\"transform\":[{\"processType\":\"dirty\",\"config\":{\"dirtyPath\":\"%s\"}},"
                    + "{\"processType\":\"schema\"}],\"sink\":[{\"processType\":\"file\",\"config\":{\"path\":\"%s\"}}]}",
            inputPath, dirtyPath, outPutPath);

    private SparkContext sc;
    private ETLContext<SparkContext> sparkContextETLContext;


    @Before
    public void init() throws IOException {
        SparkConf conf = new SparkConf().setAppName("test etl").setMaster("local[2]");
        sc = new SparkContext(conf);
        FileUtils.deleteDirectory(new File(outPutPath));
    }

    @Test
    public void start() throws Exception {
        try {
            sparkContextETLContext = new ETLContext<>(SparkETLTest.class, sc, jsonStr);
            sparkContextETLContext.start();
        } catch (Throwable ex) {
            throw ex; // 这里用户可以根据平台的业务进行异常处理
        } finally {
            if (sparkContextETLContext != null) {
                sparkContextETLContext.close();
            }
        }
    }

    @Test
    public void stop() throws Exception {
        sparkContextETLContext = new ETLContext<>(SparkETLTest.class, sc, jsonStr);
        sparkContextETLContext.close();
    }


}

