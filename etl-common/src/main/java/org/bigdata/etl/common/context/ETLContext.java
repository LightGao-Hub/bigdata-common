package org.bigdata.etl.common.context;
// CHECKSTYLE:OFF
import static org.bigdata.etl.common.enums.CommonConstants.EXECUTOR_CHECK;
import static org.bigdata.etl.common.enums.CommonConstants.FIRST;
import static org.bigdata.etl.common.enums.CommonConstants.SECOND;
import static org.bigdata.etl.common.enums.CommonConstants.THIRD;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.bigdata.etl.common.annotations.ETLExecutor;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;
import org.bigdata.etl.common.configs.ExecutorConfig;
import org.bigdata.etl.common.enums.ExecutorType;
import org.bigdata.etl.common.enums.ProcessType;
import org.bigdata.etl.common.executors.MiddleExecutor;
import org.bigdata.etl.common.executors.SinkExecutor;
import org.bigdata.etl.common.executors.SourceExecutor;
import org.bigdata.etl.common.model.ETLJSONContext;
import org.bigdata.etl.common.model.ETLJSONNode;
import org.bigdata.etl.common.utils.JacksonUtils;


/**
 *  ETL上下文构建类, 泛型需和各个接口的实现保持一致
 *  E: engine-计算框架引擎类, 例如SparkContext / FLink-env
 *  D: data-计算框架内部的执行类型, 例如spark的DataFrame / flink的DataStream
 *  C: config-为此执行器的配置类
 *
 * Author: GL
 * Date: 2022-04-21
 */
@ToString
@Slf4j
public class ETLContext<E> implements Serializable {
    private final E engine;
    private final String etlJson;
    private final Class<?> application;
    private final ETLJSONContext jsonContext;
    private final Map<String, String> executorConfigMap = new HashMap<>();
    private final Map<String, SourceExecutor<E, ?, ? extends ExecutorConfig>> sourceMap = new HashMap<>();
    private final Map<String, MiddleExecutor<E, ?, ?, ? extends ExecutorConfig>> middleMap = new HashMap<>();
    private final Map<String, SinkExecutor<E, ?, ? extends ExecutorConfig>> sinkMap = new HashMap<>();

    public ETLContext(Class<?> application, E engine, String etlJson) throws InstantiationException, IllegalAccessException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException {
        this.engine = engine;
        this.etlJson = etlJson;
        this.application = application;
        this.jsonContext = buildExecutor();   // 1、构建项目中所有执行map 2、将etlJson字符串转化成上下文
        start();
    }

    // 执行etl
    private void start() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException,
            ClassNotFoundException {
        invokeCheck();
        invokeInit();
        invokeProcess();
    }

    public void close() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        initOrClose(ExecutorType.CLOSE);
    }

    private void invokeCheck() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        final ETLJSONNode source = jsonContext.getSource();
        final SourceExecutor<E, ?, ? extends ExecutorConfig> sourceExecutor = sourceMap.get(source.getProcessType());
        final Method sourceMethod = sourceExecutor.getClass().getMethod(EXECUTOR_CHECK, source.getJsonObject().getClass());
        boolean invoke = (boolean) sourceMethod.invoke(sourceExecutor, source.getJsonObject());
        checkError(invoke, ProcessType.SOURCE, source.getProcessType());

        final List<ETLJSONNode> middleList = jsonContext.getMiddles();
        for (ETLJSONNode middle : middleList) {
            final MiddleExecutor<E, ?, ?, ? extends ExecutorConfig> middleExecutor = middleMap.get(middle.getProcessType());
            final Method middleMethod = middleExecutor.getClass().getMethod(EXECUTOR_CHECK, middle.getJsonObject().getClass());
            invoke = (boolean) middleMethod.invoke(middleExecutor, middle.getJsonObject());
            checkError(invoke, ProcessType.MIDDLE, middle.getProcessType());
        }

        final List<ETLJSONNode> sinkList = jsonContext.getSinks();
        for (ETLJSONNode sink : sinkList) {
            final SinkExecutor<E, ?, ? extends ExecutorConfig> sinkExecutor = sinkMap.get(sink.getProcessType());
            final Method sinkMethod = sinkExecutor.getClass().getMethod(EXECUTOR_CHECK, sink.getJsonObject().getClass());
            invoke = (boolean) sinkMethod.invoke(sinkExecutor, sink.getJsonObject());
            checkError(invoke, ProcessType.SINK, sink.getProcessType());
        }
    }

    private void invokeInit() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        initOrClose(ExecutorType.INIT);
    }

    private void initOrClose(ExecutorType executorType) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        final ETLJSONNode source = jsonContext.getSource();
        final SourceExecutor<E, ?, ? extends ExecutorConfig> sourceExecutor = sourceMap.get(source.getProcessType());
        final Method souceMethod = sourceExecutor.getClass().getMethod(executorType.getExecutorType(), engine.getClass(),
                source.getJsonObject().getClass());
        souceMethod.invoke(sourceExecutor, engine, source.getJsonObject());
        log.info("source[{}] {} success ", source.getProcessType(), executorType.getExecutorType());

        final List<ETLJSONNode> middleList = jsonContext.getMiddles();
        for (ETLJSONNode middle : middleList) {
            final MiddleExecutor<E, ?, ?, ? extends ExecutorConfig> middleExecutor = middleMap.get(middle.getProcessType());
            final Method middleMethod = middleExecutor.getClass().getMethod(executorType.getExecutorType(), engine.getClass(),
                    middle.getJsonObject().getClass());
            middleMethod.invoke(middleExecutor, engine, middle.getJsonObject());
            log.info("middle[{}] {} success ", middle.getProcessType(), executorType.getExecutorType());
        }

        final List<ETLJSONNode> sinkList = jsonContext.getSinks();
        for (ETLJSONNode sink : sinkList) {
            final SinkExecutor<E, ?, ? extends ExecutorConfig> sinkExecutor = sinkMap.get(sink.getProcessType());
            final Method sinkMethod = sinkExecutor.getClass().getMethod(executorType.getExecutorType(), engine.getClass(),
                    sink.getJsonObject().getClass());
            sinkMethod.invoke(sinkExecutor, engine, sink.getJsonObject());
            log.info("sink[{}] {} success ", sink.getProcessType(), executorType.getExecutorType());
        }
    }

    private void invokeProcess() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException,
            ClassNotFoundException {
        final ETLJSONNode source = jsonContext.getSource();
        final SourceExecutor<E, ?, ? extends ExecutorConfig> sourceExecutor = sourceMap.get(source.getProcessType());
        final Method souceMethod = sourceExecutor.getClass().getMethod(ExecutorType.PROCESS.getExecutorType(), engine.getClass(),
                source.getJsonObject().getClass());
        Object invokeResult = souceMethod.invoke(sourceExecutor, engine, source.getJsonObject());
        log.info("source[{}] process success, result: {}", source.getProcessType(), invokeResult);

        final List<ETLJSONNode> middleList = jsonContext.getMiddles();
        for (ETLJSONNode middle : middleList) {
            final MiddleExecutor<E, ?, ?, ? extends ExecutorConfig> middleExecutor = middleMap.get(middle.getProcessType());
            final String inputClassName = getConfig(middleExecutor.getClass(), FIRST);
            final Method middleMethod = middleExecutor.getClass().getMethod(ExecutorType.PROCESS.getExecutorType(), engine.getClass(), Class.forName(inputClassName),
                    middle.getJsonObject().getClass());
            invokeResult = middleMethod.invoke(middleExecutor, engine, invokeResult, middle.getJsonObject());
            log.info("middle[{}] process success,  result: {}", middle.getProcessType(), invokeResult);
        }

        final List<ETLJSONNode> sinkList = jsonContext.getSinks();
        for (ETLJSONNode sink : sinkList) {
            final SinkExecutor<E, ?, ? extends ExecutorConfig> sinkExecutor = sinkMap.get(sink.getProcessType());
            final String inputClassName = getConfig(sinkExecutor.getClass(), FIRST);
            final Method sinkMethod = sinkExecutor.getClass().getMethod(ExecutorType.PROCESS.getExecutorType(), engine.getClass(), Class.forName(inputClassName),
                    sink.getJsonObject().getClass());
            sinkMethod.invoke(sinkExecutor, engine, invokeResult, sink.getJsonObject());
            log.info("sink[{}], process success", sink.getProcessType());
        }
    }

    private void checkError(boolean invoke, ProcessType processType, String processTypeStr) {
        if (!invoke) {
            throw new RuntimeException(String.format("check error, processType: %s, processTypeStr: %s", processType, processTypeStr));
        }
        log.info("{}[{}], check success", processType.getProcessType(), processTypeStr);
    }

    private ETLJSONContext buildJson() throws ClassNotFoundException {
        assert etlJson != null;
        final Map<String, Object> stringObjectMap = JacksonUtils.jsonToMap(etlJson);
        if (!stringObjectMap.containsKey(ProcessType.SOURCE.getProcessType()) || !stringObjectMap.containsKey(ProcessType.SINK.getProcessType())) {
            throw new RuntimeException(String.format("etlJson error, source/sink is null etlJson: %s", etlJson));
        }
        final Object sourceValue = stringObjectMap.get(ProcessType.SOURCE.getProcessType());
        final ETLJSONNode source = getJsonNode(sourceValue);
        log.info("buildJson-source: {}", source);

        List<ETLJSONNode> middles = null; // 用户可以不使用middle
        if (stringObjectMap.containsKey(ProcessType.MIDDLE.getProcessType())) {
            final Object middleValue = stringObjectMap.get(ProcessType.MIDDLE.getProcessType());
            middles = getListJsonNode(middleValue);
            log.info("buildJson-middles: {}", middles);
        }

        final Object sinkValue = stringObjectMap.get(ProcessType.SINK.getProcessType());
        final List<ETLJSONNode> sinks = getListJsonNode(sinkValue);
        log.info("buildJson-sinks: {}", sinks);

        return new ETLJSONContext(source, middles, sinks);
    }


    private ETLJSONContext buildExecutor() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        final String packageName = this.application.getPackage().getName();
        log.info("buildExecutor package: {}", packageName);

        Reflections reflections = new Reflections(packageName);
        Set<Class<?>> classList = reflections.getTypesAnnotatedWith(ETLExecutor.class);
        for (Class<?> clz : classList) {
            String processType = clz.getAnnotation(ETLExecutor.class).value();
            Class<?>[] interfaces = clz.getInterfaces();
            boolean include = false;
            for (Class<?> interfaced : interfaces) {
                switch (interfaced.getSimpleName()) {
                    case "SourceExecutor":
                        sourceMap.put(processType, (SourceExecutor<E, ?, ExecutorConfig>) clz.newInstance());
                        executorConfigMap.put(processType, getConfig(clz, SECOND));
                        include = true;
                        break;
                    case "MiddleExecutor":
                        middleMap.put(processType, (MiddleExecutor<E, ?, ?, ExecutorConfig>) clz.newInstance());
                        executorConfigMap.put(processType, getConfig(clz, THIRD));
                        include = true;
                        break;
                    case "SinkExecutor":
                        sinkMap.put(processType, (SinkExecutor<E, ?, ExecutorConfig>) clz.newInstance());
                        executorConfigMap.put(processType, getConfig(clz, SECOND));
                        include = true;
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + interfaced.getSimpleName());
                }
            }
            if (!include) {
                throw new RuntimeException(" Using annotations but not implementing interfaces: ETLSource/ETLMiddle/ETLSink ");
            }
        }
        if (sourceMap.isEmpty() || sinkMap.isEmpty()) {
            throw new RuntimeException(String.format(" sourceMap/sinkMap Length less than 1, sourceMap-length: %s, sinkMap-length: %s",
                    sourceMap.size(), sinkMap.size()));
        }
        return buildJson();
    }

    /**
     * 根据executorClass和泛型位置, 获取对应的泛型全限定类名
     * 此函数通过嵌套判断来解决泛型中嵌套泛型, 例如MiddleExecutor[SparkSession, RDD[String], x, x], 虽然获取到了RDD[String], 但org.apache.spark.rdd.RDD<java.lang.String>字符串在外界class.forName无法反射
     * 因此需要获取内部 RDD的className = org.apache.spark.rdd.RDD
     */
    private String getConfig(Class<?> executorClass, int number) {
        final Type[] genericInterfaces = executorClass.getGenericInterfaces();
        String configName = null;
        for (Type genericInterface : genericInterfaces) {
            if (genericInterface instanceof ParameterizedType) {
                final ParameterizedType parameterizedType = (ParameterizedType) genericInterface;
                Type actualTypeArgument = parameterizedType.getActualTypeArguments()[number];
                if (actualTypeArgument instanceof ParameterizedType) {
                    configName = ((ParameterizedType) actualTypeArgument).getRawType().getTypeName();
                } else {
                    configName = actualTypeArgument.getTypeName();
                }
                log.debug("getConfig[{}], configName: {} ", executorClass, configName);
            }
        }
        if (configName == null) {
            String processType2 = executorClass.getAnnotation(ETLExecutor.class).value();
            throw new RuntimeException(String.format("executor[%s] config is not find", processType2));
        }
        return configName;
    }


    private List<ETLJSONNode> getListJsonNode(Object jsonObject) throws ClassNotFoundException {
        final List jsonObjects = JacksonUtils.convertValue(jsonObject, List.class);
        List<ETLJSONNode> list = new ArrayList<>();
        for (Object value : Objects.requireNonNull(jsonObjects)) {
            final ETLJSONNode jsonNode = getJsonNode(value);
            list.add(jsonNode);
        }
        return list;
    }

    private ETLJSONNode getJsonNode(Object value) throws ClassNotFoundException {
        ETLJSONNode etlJsonObject = JacksonUtils.convertValue(value, ETLJSONNode.class);
        assert etlJsonObject != null;
        checkJsonTypeExist(etlJsonObject);
        final Object obj = JacksonUtils.convertValue(value, Class.forName(executorConfigMap.get(etlJsonObject.getProcessType())));
        etlJsonObject.setJsonObject(obj);
        return etlJsonObject;
    }

    private void checkJsonTypeExist(ETLJSONNode etlJsonObject) {
        if (!executorConfigMap.containsKey(etlJsonObject.getProcessType())) {
            throw new RuntimeException(String.format("etlJson processType[%s] is not found, executorConfigMap: %s", etlJsonObject.getProcessType(),
                    executorConfigMap));
        }
    }

}
