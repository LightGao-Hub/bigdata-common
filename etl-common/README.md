### ETL-COMMON介绍

​    etl-common是一个可扩展的数据接入(ETL)基础框架，用户只需要编写不同的数据源(source)/业务(transform)/数据汇(sink) 的逻辑代码即可，etl-common可以根据注解自动收集source/transform/sink从而触发etl流程！

​    etl-common设计初衷是想解决多个source/transform/sink 如何易用/可扩展的整合和执行，让使用者只需关注业务代码即可，无需关注执行器的整合跟调用，避免重复造轮子。

​    etl-common可以支持基于spark/flink等多种计算引擎来实现etl流程。

### 1、框架设计

​    etl-common框架设计之初是将数据以水流的方式从上到下的流向，故将etl分为了三个部分: source 、transform、sink

![0](https://raw.githubusercontent.com/gaolight/bigdata-common/master/etl-common/picture/0.jpg)

​    source为数据源读取，一次etl过程只支持一个source

​    transform是数据流的中间操作，常用于业务操作，例如业务中常见的脏数据的处理，就可以用transform进行中间操作，一次etl过程支持多个transform操作，以水流的的方式向下传递

​    sink是数据汇写入，一次etl过程支持多个sink

​    

例如下图： 

​    数据从file文件数据源读取[file-source] -> 业务脏数据处理[dirty-transform] -> 业务类型转换处理[schema-transform] -> 写入到多个数据汇(es/hbase/hive - sink)

​    ![1](https://raw.githubusercontent.com/gaolight/bigdata-common/master/etl-common/picture/1.jpg)

### 2、参数设计

#### 2.1、介绍

​    假设用户创建了四种组件的执行器:hive、hdfs、es、hbase；此时需求是只执行从hive -> hbase的etl，

如何让etl-common知道需求并执行呢？

​    故框架需要一个执行参数，即json字符串，如下：

```json
{
    "source": {
        "processType": "file",
        "config": {
            "path": "/input"
        }
    },
    "transform": [
   {
        "processType": "dirty",
        "config": {
            "dirtyPath": "/dirty"
        }
     }, 
   {
        "processType": "schema",
        "config": {
            "dirtyPath": "/dirty"
        }
     }
  ],
    "sink": [
   {
        "processType": "hdfs",
        "config": {
            "path": "/out"
        }
     }, 
   {
        "processType": "es",
        "config": {
            "url": "....."
        }
     }, 
   {
        "processType": "hive",
        "config": {
            "sql": "....."
        }
     }
  ]
}
```

此json参数是根据图1.0所设计的json参数

此json中的processType为指定执行器的类型(对应注解参数,详细看3.4)，config则是此执行器所需要的bean类，用户可以根据自己的需求编写不同的config参数(详细看3.2)

#### 2.2、注意

##### 2.2.1、空transform

若业务中不需要数据中间处理，则可在etlJson中去掉transform，如下：

```json
{
    "source": {
        "processType": "file",
        "config": {
            "path": "/input"
        }
    },
    "sink": [{
            "processType": "hdfs",
            "config": {
                "path": "/out"
            }
        },
        {
            "processType": "es",
            "config": {
                "url": "....."
            }
        },
        {
            "processType": "hive",
            "config": {
                "sql": "....."
            }
        }
    ]
}
```

##### 2.2.2、空config

若业务中不需要config, 可以不传递config, 或config为空，如下:

```json
{
    "source": {
        "processType": "file"
    }
}
```

或: 

```json
{
    "source": {
        "processType": "file",
        "config": {}
    }
}
```

**对应的config配置类应该为NilExecutorConfig，详细看3.2.4**

**注意：config配置类需要满足javaBean，实现Serializable接口，且提供无参构造方法及变量的set/get函数**

### 3、executor执行器

#### 3.1、executor接口

##### 3.1.1、介绍

在etl-common设计中，将所有的source/transform/sink 都当做executor执行器，并提供了三种executor执行器接口，分别为: 

```java
/**
 *  执行器接口
 *  E: engine-计算框架引擎类, 例如SparkContext / FLink-env
 *    O: 计算框架内部的执行类型, 例如spark的DataFrame / flink的DataStream
 *  C: config-为此执行器的配置类
 *  I: sourceExecutor的输出类型
 *  D: data-计算框架内部的执行类型, 例如spark的DataFrame / flink的DataStream
 */
  public interface SourceExecutor<E, O, C extends Serializable> extends ETLCheck<C>, Serializable {

      void init(E engine, C config);

      O process(E engine, C config);

      void close(E engine, C config);
  }

public interface TransformExecutor<E, I, O, C extends Serializable> extends ETLCheck<C>, Serializable {

    void init(E engine, C config);

    O process(E engine, I value, C config);

    void close(E engine, C config);
}

public interface SinkExecutor<E, I, C extends Serializable> extends ETLCheck<C>, Serializable {

    void init(E engine, C config); // 泛型参数及jsonObject的值

    void process(E engine, I value, C config);

    void close(E engine, C config);
}
```

init函数用来初始化，比如: ES-Sink 可能需要先创建索引index，这部分工作可以放在init函数中。

process函数用于真正执行业务逻辑

close函数用于etl结束后的收尾工作

check函数用于做执行前的校验工作[详细看3.2]

**用户根据自己的需求分别实现三种接口，其中的泛型中需要额外关注的是O,I 两种泛型，因为source的输出是下游transform的输入，同时transform的输出也是下游sink的输入，故泛型应一一对应，如下流向：**

**SourceExecutor.process() [返回值O] --> TransformExecutor.process() [入参: I value]  ->  SinkExecutor.process [入参: I value]**

##### 3.1.2、举例

我们基于scala语言用spark引擎来实现一个从filesource -> dirtyTransform -> fileSink 的实现

暂时先忽略下面的@ETLExecutor()注解、FileConfig和check函数实现，在后面3.2 、3.3中会详细讲解

**fileSource**

```scala
@ETLExecutor("file")
class FileSourceExecutor extends SourceExecutor[SparkSession, RDD[String], FileConfig] {

  override def init(engine: SparkSession, config: FileConfig): Unit = {
  }

  override def process(engine: SparkSession, config: FileConfig): RDD[String] = {
  }

  override def close(engine: SparkSession, config: FileConfig): Unit = {
  }

  override def check(config: FileConfig): Boolean = {
  }
}
```

**dirtyTransform**

```scala
@ETLExecutor("dirty")
class DirtyTransformExecutor extends TransformExecutor[SparkSession, RDD[String], DataFrame, DirtyConfig] {

  override def init(engine: SparkSession, config: DirtyConfig): Unit = {
  }

  override def process(engine: SparkSession, value: RDD[String], config: DirtyConfig): DataFrame = {
  }

  override def close(engine: SparkSession, config: DirtyConfig): Unit = {
  }

  override def check(config: DirtyConfig): Boolean = {
  }
}
```

**fileSink**

```scala
@ETLExecutor("file")
class FileSinkExecutor extends SinkExecutor[SparkSession, DataFrame, FileConfig] {

  override def init(engine: SparkSession, config: FileConfig): Unit = {
  }

  override def process(engine: SparkSession, value: DataFrame, config: FileConfig): Unit = {
  }

  override def close(engine: SparkSession, config: FileConfig): Unit = {
  }

  override def check(config: FileConfig): Boolean = {
  }
}
```

从上面例子中可以看出fileSource 中的process函数读取文件数据后返回RDD[String]，此结果为dirtyTransform中process函数的value输入，经过dirtyTransform的脏数据处理后将数据转化为DataFrame输出，最后到达fileSink中process函数的value输入，实现文件输出，至此一个etl过程结束。

##### 3.1.3、注意

当多个sink或transform时，执行顺序是由执行参数json字符串控制的，如下：transform数组中会按照 dirtyTransform-> schemaTransform的顺序执行

```json
"transform": [
  {
        "processType": "dirty",
        "config": {
            "dirtyPath": "/dirty"
        }
    }, 
  {
        "processType": "schema",
        "config": {
            "dirtyPath": "/dirty"
        }
    }
]
```

#### 3.2、Config配置类

##### 3.2.1、介绍

​    etl-common中所有的executor都应该有对应的config配置类，用于封装json字符串中有config的配置

##### 3.2.2、举例

​    如下示例中，source-json的类型是file，config配置中只有path参数，用户需要创建对应的fileConfig配置类，并遵守javaBean规范，

继而创建FileSourceExecutor，并将对应的fileConfig 设置到泛型中

​    若FileSourceExecutor泛型不配置对应config类，则etl容器无法获取到对应的配置类，导致任务执行失败

**json:**

```json
"source": {
        "processType": "file",
        "config": {
            "path": "/input"
        }
}
```

**fileConfig:**

```scala
case class FileConfig(path: String) {
}
```

**fileSourceExecutor:**

```scala
@ETLExecutor("file")
class FileSourceExecutor extends SourceExecutor[SparkSession, RDD[String], FileConfig] {

  override def init(engine: SparkSession, config: FileConfig): Unit = {
  }

  override def process(engine: SparkSession, config: FileConfig): RDD[String] = {
  }

  override def close(engine: SparkSession, config: FileConfig): Unit = {
  }

  override def check(config: FileConfig): Boolean = {
  }

}
```

##### 3.2.3、注意

​    etl-common认为两个相同类型的executor执行器应该对应同一个config配置类；

***

​    如下: source-json 和 sink-json中的执行器类型都是file，故应该使用同一个fileConfig配置类， 若业务上需要不同的config，则应该创建不同类型的executor

**json:**

```json
"source": {
        "processType": "file",
        "config": {
            "path": "/input"
        }
}
"sink": [
   {
        "processType": "file",
        "config": {
            "path": "/out"
        }
     }
 ]
```

**fileConfig:**

```scala
case class FileConfig(path: String) {
}
```

**executor:**

```scala
@ETLExecutor("file")
class FileSourceExecutor extends SourceExecutor[SparkSession, RDD[String], FileConfig] {
    // ......
}

@ETLExecutor("file")
class FileSinkExecutor extends SinkExecutor[SparkSession, DataFrame, FileConfig] {
    // ......
}
```

##### 3.2.4、Nil

若用户的executor不需要配置类，则可以使用etl-common中内置的NilExecutorConfig，如下：

**json:**

```json
"sink": [
  {
        "processType": "print"
    }
]
```

**executor:**

```scala
@ETLExecutor("print")
class PrintSinkExecutor extends SinkExecutor[SparkSession, DataFrame, NilExecutorConfig] {
  // ......
}
```

**内置NilExecutorConfig:**

```java
/**
 *  内置空配置类
 */
public class NilExecutorConfig implements Serializable {
}
```

**可以看出json中sink无config配置信息，则executor的C泛型应设置为NilExecutorConfig，否则会执行失败**

#### 3.3、ETLCheck接口

##### 3.3.1、介绍

所有的executor都继承ETLCheck<C>接口，此接口目的是供用户进行配置校验，放置配置类参数和业务不对应；

泛型C为对应executor的C配置类泛型

##### 3.3.2、举例

**json:**

```json
"source": {
        "processType": "file",
        "config": {
            "path": "/tmp/input.txt"
        }
}
```

**executor:**

```scala
override def check(config: FileConfig): Boolean = {
    log.info("FileSourceExecutor check, config: {}", config.path)
    new File(config.path).exists() 
}
```

**用户传参的json中，fileSource配置了path目录，但是服务器上有可能并没有此文件，故用户可以在check函数中进行校验，避免程序在运行中报错，check返回值若为false，则会提前终止程序；**

#### 3.4、ETLExecutor注解

##### 3.4.1、介绍

ETLExecutor注解需要配置在Executor的实现类上，用于让ETLContext扫描，可以将此注解理解为Spring中的@Component

即: 只有使用@ETLExecuto注解的类才会被ETLContext扫描到容器中！

其中注解的value参数为此执行器的类型，对应用户的json传参中的processType

##### 3.4.2、举例

**json参数：**

```json
"source": {
        "processType": "file",
        "config": {
            "path": "/tmp/input.txt"
        }
}
```

**etl实现：**

```scala
@ETLExecutor("file")
class FileSourceExecutor extends SourceExecutor[SparkSession, RDD[String], FileConfig] {
      //.....
}
```

可以看出json中的processType 和 @ETLExecutor("file")注解中的入参保持一致。

再回到3.1.2中，可以看到在三种Executor上皆有@ETLExecuto注解，并设置了和json中processType值一致的参数；

##### 3.4.3、注意

###### 3.4.3.1、案例1

source/transform/sink中 @ETLExecutor("")注解中的入参应该保持唯一,  例如：source中不能存在两个相同@ETLExecutor("file")注解；

但是source和sink中可以各存在一个@ETLExecutor("file")注解的类

**json:**

```json
"source": {
        "processType": "file",
        "config": {
            "path": "/input"
        }
}
"sink": [
   {
        "processType": "file",
        "config": {
            "path": "/out"
        }
     }
 ]
```

**executor:**

```scala
@ETLExecutor("file")
class FileSourceExecutor extends SourceExecutor[SparkSession, RDD[String], FileConfig] {
    // ......
}

@ETLExecutor("file")
class FileSinkExecutor extends SinkExecutor[SparkSession, DataFrame, FileConfig] {
    // ......
}
```

**其原理是在etl容器中会将source/transform/sink 分别创建三个HashMap，key为注解的参数, value为对应executor**

```java
Map<String, SourceExecutor<E, ?, ? extends Serializable>> sourceMap = new HashMap<>();
Map<String, TransformExecutor<E, ?, ?, ? extends Serializable>> transformMap = new HashMap<>();
Map<String, SinkExecutor<E, ?, ? extends Serializable>> sinkMap = new HashMap<>();
```

###### 3.4.3.1、案例2

@ETLExecutor()注解参数和json参数中的process要保持一直，例如：

**json:**

```json
{
    "source": {
        "processType": "file",
        "config": {
            "path": "/input"
        }
    },
    "transform": [{
        "processType": "dirty",
        "config": {
            "dirtyPath": "/dirty"
        }
    }],
    "sink": [{}]
}
```

**executor:错误写法**

```scala
@ETLExecutor("file")
class FileSourceExecutor extends SourceExecutor[SparkSession, RDD[String], FileConfig] {
      //......
}

@ETLExecutor("file")
class DirtyTransformExecutor extends TransformExecutor[SparkSession, RDD[String], DataFrame, DirtyConfig] {
  //......
}
```

DirtyTransformExecutor的@ETLExecutor()入参应该是dirty，此时设置的是file，当json传参执行时会找不到对应执行器从而报错

### 4、ETLContext

#### 4.1、介绍

​    ETLContext是etl的核心类，相当于spring中的applicationContext，创建此类时会扫描当前类所在位置下的所有带@ETLExecutor注解

并开始构建etl容器，创建完成后用户可以调用start函数即可开始etl流程。

#### 4.2、使用

这里我们用spark作为执行引擎举例

**scala:**

```scala
val etl = new ETLContext[SparkSession](classOf[SparkETLTest], spark, etlJson)
etl.start()
```

**java:**

```java
ETLContext<sparkSession> sparkContextETLContext = new ETLContext<>(SparkETLTest.class, sc, etlJson);
sparkContextETLContext.start();
```

ETLContext需要三个参数：当前main函数所在类的class对象，执行引擎的核心类[此处是sparkSession]，etlJson字符串

此类对外提供start/stop两种接口，当创建成功后调用start函数即可完成一次etl过程

若程序执行异常时也可以通过stop进行收尾工作。

#### 4.4、ETLContext执行顺序

![2](https://raw.githubusercontent.com/gaolight/bigdata-common/master/etl-common/picture/2.jpg)

#### 4.4、注意

用户创建的程序主类应该放在包名根目录下, 如下：

![3](https://raw.githubusercontent.com/gaolight/bigdata-common/master/etl-common/picture/3.jpg)

### 6、快速使用

#### 6.1、pom依赖

**通过git下载源码并install到本地仓库**

```shell
# 先打包etl-common项目到maven仓库
mkdir gitEtl
cd gitEtl
git clone https://github.com/gaolight/bigdata-common.git
cd bigdata-common
mvn -U clean install -DskipTests
```

然后在项目中依赖etl-common包即可使用

```xml
<dependency>
      <groupId>org.bigdata</groupId>
      <artifactId>etl-common</artifactId>
      <version>1.0</version>
</dependency>
```

#### 6.2、Demo

##### 6.2.1、java

[https://github.com/gaolight/bigdata-common/blob/master/etl-common/src/test/java/org/bigdata/etl/common/java/test/SparkETLTest.java](https://github.com/gaolight/bigdata-common/blob/master/etl-common/src/test/java/org/bigdata/etl/common/java/test/SparkETLTest.java)

##### 6.2.2、scala

###### 6.2.2.1、spark

[https://github.com/gaolight/bigdata-common/blob/master/etl-common/src/test/scala/org/bigdata/etl/common/scala/test/spark/SparkETLTest.scala](https://github.com/gaolight/bigdata-common/blob/master/etl-common/src/test/scala/org/bigdata/etl/common/scala/test/spark/SparkETLTest.scala)

###### 6.2.2.2、flink

[https://github.com/gaolight/bigdata-common/blob/master/etl-common/src/test/scala/org/bigdata/etl/common/scala/test/flink/FlinkETLTest.scala](https://github.com/gaolight/bigdata-common/blob/master/etl-common/src/test/scala/org/bigdata/etl/common/scala/test/flink/FlinkETLTest.scala)

#### 6.3、注意

由于etl-common中使用了jackson来进行json解析，故在使用时可能会和spark版本或其他计算引擎造成冲突

若有jar包冲突问题，建议通过exclusion剔除对应包后使用

#### 7、调优

##### 7.1、spark

###### 7.1.1、算子缓存

应用中如果有多个sink，且多个sink使用同一个上游rdd/dataframe算子时，建议将同一上游算子缓存，避免多sink-action操作时重复计算，如下图：

![](https://tva1.sinaimg.cn/large/e6c9d24egy1h3ur1e978zj20gf0akmxl.jpg)

###### 7.1.2、sink并发写入

应用中sink写入算子尽量根据下游数据源来手动设置合适的分区数量【spark.sql.shuffle.partitions】，例如sink为elasticSearch时，如果不主动设置分区数量，按照spark-sql默认的并发会是200分区，而200并发写入同es效率并不高；

##### 7.2、flink

## 开发readme

#### 运行checkstyle校验

```shell
mvn clean validate -Dforce.refresh.release=true
```

#### 跳过checkstyle校验编译

```shell
mvn clean compile -Dcheckstyle.skip=true
```

#### install打包方式

```shell
mvn -U  clean install  -DskipTests
```

#### 子父工程版本修改

```shell
mvn versions:set -DnewVersion=2.0
```
