package org.bigdata.etl.common.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import com.sun.istack.internal.NotNull;
import lombok.extern.slf4j.Slf4j;

/**
 * Created by chengmo on 2018/4/12.
 * update by xuff 2020/03/11 remove fastjson
 */
@Slf4j
public final class JacksonUtils {

    private static final ObjectMapper MAPPER = JsonMapper.builder().addModule(new DefaultScalaModule()).build();

    static {
        MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        MAPPER.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        // 枚举属性忽略大小写
        MAPPER.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_ENUMS, true);
    }

    private JacksonUtils() {
    }

    public static <T> T convertValue(Object obj, Class<T> tClass) {
        try {
            return MAPPER.convertValue(obj, tClass);
        } catch (Exception e) {
            log.warn("jackson convert object [{}] error:{}", obj, e.getMessage(), e);
        }
        return null;
    }

    public static <T> T convertValue(Object obj, TypeReference<T> toValueTypeRef) {
        try {
            return MAPPER.convertValue(obj, toValueTypeRef);
        } catch (Exception e) {
            log.warn("jackson convert object [{}] error:{}", obj, e.getMessage(), e);
        }
        return null;
    }

    public static Map<String, Object> parseObject(String json) {
        try {
            return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception e) {
            log.warn("jackson convert json [{}] error:{}", json, e.getMessage(), e);
        }
        return null;
    }


    public static <T> T parseObject(String json, Class<T> tClass) {
        try {
            return MAPPER.readValue(json, tClass);
        } catch (Exception e) {
            e.printStackTrace();
            log.warn("jackson convert json [{}] error:{}", json, e.getMessage(), e);
        }
        return null;
    }

    public static <T> T parseObjectWithType(String json, TypeReference<T> typeRef) {
        try {
            return MAPPER.readValue(json, typeRef);
        } catch (Exception e) {
            log.warn("jackson convert json [{}] error:{}", json, e.getMessage(), e);
        }
        return null;
    }

    /*public static <T> T parseObject(String json, ParameterizedTPypeReference<T> typeRef) {
        try {
            return mapper.readValue(json, mapper.getTypeFactory().constructType(typeRef.getType()));
        } catch (Exception e) {
            log.warn("jackson convert json [{}] error:{}", json, e.getMessage(), e);
        }
        return null;
    }*/

    public static <T> List<T> parseArray(String readTxt, Class<T> cls) {
        try {
            return MAPPER.readValue(readTxt, MAPPER.getTypeFactory().constructCollectionLikeType(List.class, cls));
        } catch (Exception e) {
            log.warn("jackson convert array json [{}] error:{}", readTxt, e.getMessage(), e);
        }
        return Collections.emptyList();
    }


    // 不应当放在这里 反射可能性能更好
    @Deprecated
    public static Map<String, Object> toMap(@NotNull Object bean) {
        //如果就是String或者相关的类 就按照字符串转换
        if (bean.getClass().isAssignableFrom(String.class)) {
            return jsonToMap(bean.toString());
        }
        try {
            return MAPPER.readValue(MAPPER.writeValueAsBytes(bean), new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception e) {
            log.warn("JackSon convert object [{}] error:{}", bean, e.getMessage(), e);
        }
        return Collections.emptyMap();
    }

    public static List<Map<String, Object>> toListMap(@NotNull List<?> beanList) {
        try {
            return MAPPER.readValue(MAPPER.writeValueAsBytes(beanList), new TypeReference<List<Map<String, Object>>>() {
            });
        } catch (Exception e) {
            log.warn("JackSon convert list [{}] error:{}", beanList, e.getMessage(), e);
        }
        return Collections.emptyList();
    }

    public static Map<String, Object> jsonToMap(@NotNull String json) {
        try {
            return MAPPER.readValue(json, new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception e) {
            log.warn("JackSon convert json [{}] error:{}", json, e.getMessage(), e);
        }
        return Collections.emptyMap();
    }

    public static List<Map<String, Object>> jsonToListMap(String json) {
        try {
            return MAPPER.readValue(json, new TypeReference<List<Map<String, Object>>>() {
            });
        } catch (Exception e) {
            log.warn("JackSon convert json [{}] error:{}", json, e.getMessage(), e);
        }
        return Collections.emptyList();
    }


    /* public static List<Map<String, Object>> toListMap(Object obj) {
        String json = JSON.toJSONString(obj);
        return JSON.parseObject(json, new TypeReference<List<Map<String, Object>>>() {
        });
    }*/
    // 反射可能性能更好
    public static <F, T> T copy(@NotNull F bean, @NotNull Class<T> toClass) {
        try {
            return MAPPER.readValue(MAPPER.writeValueAsBytes(bean), toClass);
        } catch (Exception e) {
            log.warn("JackSon convert object [{}] error:{}", bean, e.getMessage(), e);
        }
        return null;
    }

    public static <From, To> List<To> copy(List<From> beanList, Class<To> toClass) {
        List<To> toBeanList = new ArrayList<>();
        beanList.stream().forEach(e -> {
            toBeanList.add(copy(e, toClass));
        });
        return toBeanList;
    }

    public static String toJSONString(@NotNull Object object, boolean prettyPrinter) {
        try {
            return MAPPER.writer().withDefaultPrettyPrinter().writeValueAsString(object);
        } catch (Exception e) {
            log.warn("JackSon convert object [{}] error:{}", object.toString(), e.getMessage(), e);
        }
        return null;
    }

    public static String toJSONString(Object object) {
        return toJSONStringNullable(object);
    }

    public static String toJSONStringNullable(Object object) {
        if (object == null) {
            return null;
        }
        try {
            return MAPPER.writer().writeValueAsString(object);
        } catch (Exception e) {
            log.warn("JackSon convert object [{}] error:{} ", object.toString(), e.getMessage(), e);
        }
        return null;
    }

    public static String toJSONStringNotNullable(@NotNull Object object) {
        try {
            return MAPPER.writer().writeValueAsString(object);
        } catch (Exception e) {
            log.warn("JackSon convert object [{}] error:{} ", object.toString(), e.getMessage(), e);
        }
        return null;
    }

    public static Object fromJSONString(@NotNull String jsonStr, Class<?> className) {
        try {
            return MAPPER.readValue(jsonStr, className);
        } catch (Exception e) {
            log.warn("JackSon convert object [{}] error:{}", jsonStr, e.getMessage(), e);
        }
        return null;
    }

    public Object fromJSONString(@NotNull String jsonStr, TypeReference<?> typeRef) {
        try {
            return MAPPER.readValue(jsonStr, typeRef);
        } catch (Exception e) {
            log.warn("JackSon convert object [{}] error:{}", jsonStr, e.getMessage(), e);
        }
        return null;
    }


    /**
     * @description 因为是数据量大的时候，fastJson容易内存溢出
     * @author xumin
     * @date 2019/4/24
     */
    public static <T> String jsonObjectToStringByJackson(T obj) {
        String t = "";
        try {
            t = MAPPER.writeValueAsString(obj);
        } catch (Exception e) {
            log.warn("convert object [{}] to string error:{}", obj, e.getMessage(), e);
        }
        return t;
    }

    /**
     * @description 因为是数据量大的时候，fastJson转换大对象的时候很吃内存，一直把内存持有，没有快速释放内存，很容易引起内存溢出
     * 故引入了JaskSon来时间换空间
     * @author xumin
     * @date 2019/4/24
     */
    public static <T> T jsonStringToObjectByJackson(String jsonStr, Class<T> tClass) {
        T t = null;
        try {
            t = MAPPER.readValue(jsonStr, tClass);
        } catch (Exception e) {
            log.warn("JackSon convert object [{}] error:{}", jsonStr, e.getMessage(), e);
        }
        return t;
    }

    public static <T> void printPretty(T object) {
        try {
            log.info(MAPPER.writer().withDefaultPrettyPrinter().writeValueAsString(object));
        } catch (Exception e) {
            log.warn(e.getLocalizedMessage(), e);
            e.printStackTrace();
        }
    }

    public static <T> void println(T object) {
        try {
            log.info(MAPPER.writeValueAsString(object));
        } catch (Exception e) {
            log.warn(e.getLocalizedMessage(), e);
        }
    }


    public static <T1, T2> T1 deepCopyByJson(final T2 source, Class<T1> tClass) {
        String json = JacksonUtils.toJSONString(source);
        return JacksonUtils.parseObject(json, tClass);
    }


    public static <T1, T2> List<T1> deepCopyByJson(final List<T2> source, Class<T1> tClass) {
        List<T1> result = new ArrayList<>();
        for (T2 one : source) {
            result.add(deepCopyByJson(one, tClass));
        }
        return result;
    }

    public static ObjectNode createNode() {
        return MAPPER.createObjectNode();
    }

    public static ArrayNode createArrayNode() {
        return MAPPER.createArrayNode();
    }

}
