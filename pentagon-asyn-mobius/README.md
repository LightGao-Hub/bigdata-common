## 简介
    用于pentagon 与 mobius 之间异步通信
    

### 编译
```shell script
mvn -U  clean compile install -DskipTests -Pscala-2.13.5
mvn -U  clean compile install -DskipTests -Pscala-2.11.8
```
### 打包
```shell script
mvn -U  clean compile package -DskipTests -Pscala-2.13.5
mvn -U  clean compile package -DskipTests -Pscala-2.11.8
```
### 私服-分为北研和深研
```shell script
mvn deploy -DskipTests -Pbeiyan -Pscala-2.13.5
mvn deploy -DskipTests -Pbeiyan -Pscala-2.11.8
mvn deploy -DskipTests -Pshenyan -Pscala-2.13.5
mvn deploy -DskipTests -Pshenyan -Pscala-2.11.8
```