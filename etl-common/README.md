## etl-common介绍：
    
    

## 关键点：

    sparkContext 在进行一个action做的时候dirver端会阻塞指导此次action结束后再进行剩下的代码操作
    
    而etl中多个sink都是使用一个父rdd或dataframe, 故此处应该是线程池执行spark的多个sink的action操作，这样可以并行执行写入，而非顺序执行一个sink等待成功后再执行下一个sink
    
    但是！以上是spark的场景，但在flink中，由于flink的env是先设计中间的操作，多个sink不断的addSink即可，最后执行一个 start操作，并不会像spark一样阻塞掉
    
    故为了兼容两种引擎，应该使用通用的设计，即一个sink一个sink的顺序执行！
    
