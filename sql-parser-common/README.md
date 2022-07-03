## sql-parser-common介绍：
    
    随着大数据技术的发展，国内大部分一线企业都开始自研大数据平台/流式平台/离线平台，在大数据平台产品中由于底层执行引擎种类较多,
    如常见的kylin/clickhouse/hive/spark等，怎样让用户可以通过简单的方式来表达执行思路已经是常见的痛点,
    好在这些开源框架都对外提供了sql语法，这使得用户只需要针对单独的执行引擎进行查询即可,
    但每种数据库的方言截然不同，就像mysql的字符串截取SUBSTRING函数 到了oracle则必须要换成SUBSTR函数;
    
    SQL方言问题已经成了平台层面上常见的问题，针对这类问题常见的解决思路是对外采用一致的标准SQL语法，对用户完全屏蔽底层引擎,
    通过开发一个SQL语法解析服务, 将标准SQL解析成各类引擎方言, 从而实现一套SQL, 到处执行;
    
## 服务架构图：![github](https://raw.githubusercontent.com/gl0726/bigdata-common/master/sql-parser-common/picture/struct.jpg"github")

