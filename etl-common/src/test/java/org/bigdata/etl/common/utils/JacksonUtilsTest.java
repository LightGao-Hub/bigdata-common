package org.bigdata.etl.common.utils;

import lombok.extern.slf4j.Slf4j;
import org.bigdata.etl.common.configs.FileConfig;
import org.bigdata.etl.common.model.ETLJSONNode;
import org.bigdata.etl.common.enums.ProcessType;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Author: GL
 * Date: 2022-04-24
 */
@Slf4j
public class JacksonUtilsTest {

    private final String jsonStr = "{\"source\":{\"processType\":\"hdfs\",\"path\":\"/input\"},"
            + "\"transform\":[{\"processType\":\"dirty\",\"dirtyPath\":\"/dirty\"},{\"processType\":\"dirty2\","
            + "\"dirtyPath\":\"/dirty2\"}],\"sink\":[{\"processType\":\"hdfs\",\"path\":\"/out\"}]}";

    private final Map<String, Object> stringObjectMap = JacksonUtils.jsonToMap(jsonStr);


    @Test
    public void jsonToMap() {
        log.info("jsonToMap: {}", stringObjectMap);
    }

    @Test
    public void convertValue() {
        final FileConfig source = JacksonUtils.convertValue(stringObjectMap.get(ProcessType.SOURCE.getProcessType()), FileConfig.class);
        log.info("source: {}", source);
    }

    @Test
    public void convertSource() {
        final Object sourceValue = stringObjectMap.get(ProcessType.SOURCE.getProcessType());
        final ETLJSONNode etlJsonObject = JacksonUtils.convertValue(sourceValue, ETLJSONNode.class);
        assert etlJsonObject != null;
        etlJsonObject.setConfig(sourceValue);
        log.info("source: {}", etlJsonObject);
    }

    @Test
    public void converTransform() {
        final Object transformValue = stringObjectMap.get(ProcessType.TRANSFORM.getProcessType());
        log.info("transforms: {}", getListJsonNode(transformValue));
    }

    @Test
    public void convertSink() {
        final Object sinkValue = stringObjectMap.get(ProcessType.SINK.getProcessType());
        log.info("sinks: {}", getListJsonNode(sinkValue));
    }

    private List<ETLJSONNode> getListJsonNode(Object jsonObject) {
        final List jsonObjects = JacksonUtils.convertValue(jsonObject, List.class);
        List<ETLJSONNode> list = new ArrayList<>();
        for (Object value : jsonObjects) {
            ETLJSONNode etlJsonObject = JacksonUtils.convertValue(value, ETLJSONNode.class);
            assert etlJsonObject != null;
            etlJsonObject.setConfig(value);
            list.add(etlJsonObject);
        }
        return list;
    }

}
