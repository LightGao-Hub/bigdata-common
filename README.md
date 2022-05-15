## bigdata-common介绍：
    
    此项目做为大数据领域公共包, 用于存放大数据领域常见技术Demo, 并模块的方式进行分类；
    
    由于初建，目前技术分类较少，后面会慢慢补充，介绍一下分类
    
    1、clickhouse-common ： clickhouse公共模块，暂时只有连接Demo。
    
    2、sql-parser-common ： SQL语法树解析模块，提供方言解析&自定义方言Demo.
    
    3、redisson-common ： 基于redisson的redis工具类.
    
    4、etl-common ： 可扩展的数据接入(ETL)基础框架.
    
#### 运行checkstyle校验

``` shell
mvn clean validate -Dforce.refresh.release=true
```



#### 跳过checkstyle校验编译

``` shell
mvn clean compile -Dcheckstyle.skip=true
```



#### install打包方式

```shell
mvn -U  clean install  -DskipTests
```



#### 子父工程版本修改

``` shell
mvn versions:set -DnewVersion=2.0
```


## [版本功能迭代记录](doc/release-note.md)
    
            


