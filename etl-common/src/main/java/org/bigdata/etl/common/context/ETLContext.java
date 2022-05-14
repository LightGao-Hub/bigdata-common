package org.bigdata.etl.common.context;

import static org.bigdata.etl.common.enums.CommonConstants.EMPTY_JSON_STRING;
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

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;

import org.bigdata.etl.common.annotations.ETLExecutor;
import org.bigdata.etl.common.configs.NilExecutorConfig;
import org.bigdata.etl.common.enums.ExecutorType;
import org.bigdata.etl.common.enums.ProcessType;
import org.bigdata.etl.common.exceptions.AnnotationInterfaceMismatchException;
import org.bigdata.etl.common.exceptions.BuildContextException;
import org.bigdata.etl.common.exceptions.ETLStartException;
import org.bigdata.etl.common.exceptions.ETLStopException;
import org.bigdata.etl.common.exceptions.ExecutorCheckException;
import org.bigdata.etl.common.exceptions.ExecutorConfigNotFoundException;
import org.bigdata.etl.common.exceptions.ExecutorProcessTypeNotFoundException;
import org.bigdata.etl.common.exceptions.JsonTypeLackException;
import org.bigdata.etl.common.exceptions.LeastSourceSinkException;
import org.bigdata.etl.common.executors.SinkExecutor;
import org.bigdata.etl.common.executors.SourceExecutor;
import org.bigdata.etl.common.executors.TransformExecutor;
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
    private final Map<String, SourceExecutor<E, ?, ? extends Serializable>> sourceMap = new HashMap<>();
    private final Map<String, TransformExecutor<E, ?, ?, ? extends Serializable>> transformMap = new HashMap<>();
    private final Map<String, SinkExecutor<E, ?, ? extends Serializable>> sinkMap = new HashMap<>();

    public ETLContext(Class<?> application, E engine, String etlJson) throws BuildContextException {
        try {
            this.engine = engine;
            this.etlJson = etlJson;
            this.application = application;
            this.jsonContext = buildExecutor();   // 1、构建项目中所有执行map 2、将etlJson字符串转化成上下文
        } catch (Throwable e) {
            throw new BuildContextException("etlContext build error", e);
        }
    }

    // 执行etl
    public void start() throws ETLStartException {
        try {
            invokeCheck();
            invokeInit();
            invokeProcess();
        } catch (Throwable e) {
            throw new ETLStartException("etl start error", e);
        }
    }

    public void stop() throws ETLStopException {
        try {
            initOrClose(ExecutorType.CLOSE);
        } catch (Throwable e) {
            throw new ETLStopException("etl stop error", e);
        }
    }

    private void invokeCheck() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        final ETLJSONNode source = jsonContext.getSource();
        final SourceExecutor<E, ?, ? extends Serializable> sourceExecutor = sourceMap.get(source.getProcessType());
        final Method sourceMethod = sourceExecutor.getClass().getMethod(EXECUTOR_CHECK, source.getConfig().getClass());
        boolean invoke = (boolean) sourceMethod.invoke(sourceExecutor, source.getConfig());
        checkError(invoke, ProcessType.SOURCE, source.getProcessType());

        final List<ETLJSONNode> transformList = jsonContext.getTransforms();
        for (ETLJSONNode transform : transformList) {
            final TransformExecutor<E, ?, ?, ? extends Serializable> transformExecutor = transformMap.get(transform.getProcessType());
            final Method transformMethod = transformExecutor.getClass().getMethod(EXECUTOR_CHECK, transform.getConfig().getClass());
            invoke = (boolean) transformMethod.invoke(transformExecutor, transform.getConfig());
            checkError(invoke, ProcessType.TRANSFORM, transform.getProcessType());
        }

        final List<ETLJSONNode> sinkList = jsonContext.getSinks();
        for (ETLJSONNode sink : sinkList) {
            final SinkExecutor<E, ?, ? extends Serializable> sinkExecutor = sinkMap.get(sink.getProcessType());
            final Method sinkMethod = sinkExecutor.getClass().getMethod(EXECUTOR_CHECK, sink.getConfig().getClass());
            invoke = (boolean) sinkMethod.invoke(sinkExecutor, sink.getConfig());
            checkError(invoke, ProcessType.SINK, sink.getProcessType());
        }
    }

    private void invokeInit() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        initOrClose(ExecutorType.INIT);
    }

    private void initOrClose(ExecutorType executorType) throws InvocationTargetException, IllegalAccessException, NoSuchMethodException {
        final ETLJSONNode source = jsonContext.getSource();
        final SourceExecutor<E, ?, ? extends Serializable> sourceExecutor = sourceMap.get(source.getProcessType());
        final Method souceMethod = sourceExecutor.getClass().getMethod(executorType.getExecutorType(), engine.getClass(),
                source.getConfig().getClass());
        souceMethod.invoke(sourceExecutor, engine, source.getConfig());
        log.info("source[{}] {} success ", source.getProcessType(), executorType.getExecutorType());

        final List<ETLJSONNode> transformList = jsonContext.getTransforms();
        for (ETLJSONNode transform : transformList) {
            final TransformExecutor<E, ?, ?, ? extends Serializable> transformExecutor = transformMap.get(transform.getProcessType());
            final Method transformMethod = transformExecutor.getClass().getMethod(executorType.getExecutorType(), engine.getClass(),
                    transform.getConfig().getClass());
            transformMethod.invoke(transformExecutor, engine, transform.getConfig());
            log.info("transform[{}] {} success ", transform.getProcessType(), executorType.getExecutorType());
        }

        final List<ETLJSONNode> sinkList = jsonContext.getSinks();
        for (ETLJSONNode sink : sinkList) {
            final SinkExecutor<E, ?, ? extends Serializable> sinkExecutor = sinkMap.get(sink.getProcessType());
            final Method sinkMethod = sinkExecutor.getClass().getMethod(executorType.getExecutorType(), engine.getClass(),
                    sink.getConfig().getClass());
            sinkMethod.invoke(sinkExecutor, engine, sink.getConfig());
            log.info("sink[{}] {} success ", sink.getProcessType(), executorType.getExecutorType());
        }
    }

    private void invokeProcess() throws InvocationTargetException, IllegalAccessException, NoSuchMethodException,
            ClassNotFoundException {
        final ETLJSONNode source = jsonContext.getSource();
        final SourceExecutor<E, ?, ? extends Serializable> sourceExecutor = sourceMap.get(source.getProcessType());
        final Method souceMethod = sourceExecutor.getClass().getMethod(ExecutorType.PROCESS.getExecutorType(), engine.getClass(),
                source.getConfig().getClass());
        Object invokeResult = souceMethod.invoke(sourceExecutor, engine, source.getConfig());
        log.info("source[{}] process success, result: {}", source.getProcessType(), invokeResult);

        final List<ETLJSONNode> transformList = jsonContext.getTransforms();
        for (ETLJSONNode transform : transformList) {
            final TransformExecutor<E, ?, ?, ? extends Serializable> transformExecutor = transformMap.get(transform.getProcessType());
            final String inputClassName = getConfig(transformExecutor.getClass(), FIRST);
            final Method transformMethod = transformExecutor.getClass().getMethod(ExecutorType.PROCESS.getExecutorType(), engine.getClass(),
                    Class.forName(inputClassName), transform.getConfig().getClass());
            invokeResult = transformMethod.invoke(transformExecutor, engine, invokeResult, transform.getConfig());
            log.info("transform[{}] process success,  result: {}", transform.getProcessType(), invokeResult);
        }

        final List<ETLJSONNode> sinkList = jsonContext.getSinks();
        for (ETLJSONNode sink : sinkList) {
            final SinkExecutor<E, ?, ? extends Serializable> sinkExecutor = sinkMap.get(sink.getProcessType());
            final String inputClassName = getConfig(sinkExecutor.getClass(), FIRST);
            final Method sinkMethod = sinkExecutor.getClass().getMethod(ExecutorType.PROCESS.getExecutorType(), engine.getClass(),
                    Class.forName(inputClassName), sink.getConfig().getClass());
            sinkMethod.invoke(sinkExecutor, engine, invokeResult, sink.getConfig());
            log.info("sink[{}], process success", sink.getProcessType());
        }
    }

    private void checkError(boolean invoke, ProcessType processType, String processTypeStr) {
        if (!invoke) {
            throw new ExecutorCheckException(String.format("check error, processType: %s, processTypeStr: %s", processType, processTypeStr));
        }
        log.info("{}[{}], check success", processType.getProcessType(), processTypeStr);
    }

    private ETLJSONContext buildJson() throws ClassNotFoundException {
        assert etlJson != null;
        final Map<String, Object> stringObjectMap = JacksonUtils.jsonToMap(etlJson);
        if (!stringObjectMap.containsKey(ProcessType.SOURCE.getProcessType()) || !stringObjectMap.containsKey(ProcessType.SINK.getProcessType())) {
            throw new JsonTypeLackException(String.format("etlJson error, source/sink is null etlJson: %s", etlJson));
        }
        final Object sourceValue = stringObjectMap.get(ProcessType.SOURCE.getProcessType());
        final ETLJSONNode source = getJsonNode(sourceValue);
        log.info("buildJson-source: {}", source);

        List<ETLJSONNode> transforms = null; // 用户可以不使用transform
        if (stringObjectMap.containsKey(ProcessType.TRANSFORM.getProcessType())) {
            final Object transformValue = stringObjectMap.get(ProcessType.TRANSFORM.getProcessType());
            transforms = getListJsonNode(transformValue);
            log.info("buildJson-transforms: {}", transforms);
        }

        final Object sinkValue = stringObjectMap.get(ProcessType.SINK.getProcessType());
        final List<ETLJSONNode> sinks = getListJsonNode(sinkValue);
        log.info("buildJson-sinks: {}", sinks);

        return new ETLJSONContext(source, transforms, sinks);
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
                        sourceMap.put(processType, (SourceExecutor<E, ?, Serializable>) clz.newInstance());
                        executorConfigMap.put(processType, getConfig(clz, SECOND));
                        include = true;
                        break;
                    case "TransformExecutor":
                        transformMap.put(processType, (TransformExecutor<E, ?, ?, Serializable>) clz.newInstance());
                        executorConfigMap.put(processType, getConfig(clz, THIRD));
                        include = true;
                        break;
                    case "SinkExecutor":
                        sinkMap.put(processType, (SinkExecutor<E, ?, Serializable>) clz.newInstance());
                        executorConfigMap.put(processType, getConfig(clz, SECOND));
                        include = true;
                        break;
                    default:
                        throw new IllegalStateException("Unexpected value: " + interfaced.getSimpleName());
                }
            }
            if (!include) {
                throw new AnnotationInterfaceMismatchException(" Using annotations but not implementing interfaces: ETLSource/ETLtransform/ETLSink ");
            }
        }
        if (sourceMap.isEmpty() || sinkMap.isEmpty()) {
            throw new LeastSourceSinkException(String.format(" sourceMap/sinkMap Length less than 1, sourceMap-length: %s, sinkMap-length: %s",
                    sourceMap.size(), sinkMap.size()));
        }
        return buildJson();
    }

    /**
     * 根据executorClass和泛型位置, 获取对应的泛型全限定类名
     * 此函数通过嵌套判断来解决泛型中嵌套泛型, 例如transformExecutor[SparkSession, RDD[String], x, x], 虽然获取到了RDD[String],
     * 但org.apache.spark.rdd.RDD<java.lang.String>字符串在外界class.forName无法反射
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
                log.info("getConfig[{}], configName: {} ", executorClass, configName);
            }
        }
        if (configName == null) {
            throw new ExecutorConfigNotFoundException(String.format("executor[%s] config is not find",
                    executorClass.getAnnotation(ETLExecutor.class).value()));
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
        Object obj;
        if (Objects.nonNull(etlJsonObject.getConfig())) {
            obj = JacksonUtils.convertValue(etlJsonObject.getConfig(), Class.forName(executorConfigMap.get(etlJsonObject.getProcessType())));
        } else {
            obj = JacksonUtils.parseObject(EMPTY_JSON_STRING, NilExecutorConfig.class);
        }
        etlJsonObject.setConfig(obj);
        return etlJsonObject;
    }

    private void checkJsonTypeExist(ETLJSONNode etlJsonObject) {
        if (!executorConfigMap.containsKey(etlJsonObject.getProcessType()) || !(sourceMap.containsKey(etlJsonObject.getProcessType())
                || transformMap.containsKey(etlJsonObject.getProcessType()) || sinkMap.containsKey(etlJsonObject.getProcessType()))) {
            throw new ExecutorProcessTypeNotFoundException(String.format("etlJson processType[%s] is not found", etlJsonObject.getProcessType()));
        }
    }

}
