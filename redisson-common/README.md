## redisson-common介绍：
    
    此模块提供了已apache-redisson开源包为基础的工具类, 包含redis常用的 key/list/hash/set/sortSet api操作
    
    另外工具类也提供了分布式锁函数[支持自动续约&可重入], 通过lambda表达式传参, 实现了代码的简洁性;

## RedissonUtils工具类使用：

    RedissonUtils为单例模式, 可以通过getInstance(RedissonClient redisson)函数传递一个redisson 参数构建RedissonUtils实例;
    
    由于用户的redis环境部署各不相同, 所以RedissonClient的具体实现方式由用户创建, 使用者根据自身redis情况构建符合环境的RedissonClient再传递给RedissonUtils即可;
    
## RedissonClient创建：

    1、config创建：
    public void init() {
        // 配置序列化, 建议主动声明默认序列化为JsonJacksonCodec, 方便查看
        Config config = new Config().setCodec(new JsonJacksonCodec());
        // 使用单机Redis服务
        config.useSingleServer().setAddress("redis://127.0.0.1:6379");
        // 创建Redisson客户端
        RedissonClient redisson = Redisson.create(config);
        this.redissonUtils = RedissonUtils.getInstance(Optional.ofNullable(redisson));
    }
    
    2、yml创建[参考TestRedisson测试类]: 
    public void init() throws IOException {
        final URL resource = TestRedisson.class.getClassLoader().getResource("redisson.yml");
        final RedissonClient redissonClient = Redisson.create(Config.fromYAML(resource));
        this.redissonUtils = RedissonUtils.getInstance(Optional.ofNullable(redissonClient));
    }
    
    3、spring框架集成：
    @Bean(destroyMethod="shutdown")
    RedissonClient redisson() throws IOException {
        Config config = new Config();
        config.useClusterServers()
              .addNodeAddress("127.0.0.1:7004", "127.0.0.1:7001");
        return Redisson.create(config);
    }


redisson支持多种方式创建, 更多方式参考官网: https://github.com/redisson/redisson/wiki/%E7%9B%AE%E5%BD%95
yml配置参考官网: https://github.com/redisson/redisson/wiki/2.-%E9%85%8D%E7%BD%AE%E6%96%B9%E6%B3%95#221-%E9%80%9A%E8%BF%87yaml%E6%A0%BC%E5%BC%8F%E9%85%8D%E7%BD%AE

