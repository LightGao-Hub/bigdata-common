package org.bigdata.redisson.common.utils;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.bigdata.redisson.common.enums.CommonConstants.END_INDEX;
import static org.bigdata.redisson.common.enums.CommonConstants.FIRST;
import static org.bigdata.redisson.common.enums.CommonConstants.LOCK_WAIT_TIME_MILLIS;
import static org.bigdata.redisson.common.enums.CommonConstants.LOCK_WAIT_TIME_SECOND;
import static org.bigdata.redisson.common.enums.CommonConstants.SECOND;
import static org.bigdata.redisson.common.enums.CommonConstants.THIRD;
import static org.bigdata.redisson.common.enums.CommonConstants.ZERO;

import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * RedissonUtils单元测试类，所有测试均支持幂等性，可多次执行
 * Author: GL
 * Date: 2022-03-20
 */
@Slf4j
public class TestRedisson {

    private RedissonUtils redissonUtils;

    @Before
    public void init() throws IOException {
        final URL resource = TestRedisson.class.getClassLoader().getResource("redisson.yml");
        final RedissonClient redissonClient = Redisson.create(Config.fromYAML(resource));
        this.redissonUtils = RedissonUtils.getInstance(Optional.ofNullable(redissonClient));
    }

    /**
     * 测试redisson所有函数
     */
    @Test
    public void redissonAll() {
        redissonString();
        redissonList();
        redissonHash();
        redissonSortedSet();
    }

    /**
     * 测试redisson-String函数
     */
    @Test
    public void redissonString() {
        final String key = "redisson:key";
        final String value = "redisson-string-value";
        log.info("<------------ ");
        log.info(String.format("del key: %s, boolean: %s", key, redissonUtils.del(key)));

        redissonUtils.set(key, value);
        log.info(String.format("set key: %s, value: %s", key, value));
        log.info(String.format("get key: %s, value: %s", key, redissonUtils.get(key)));
        log.info(String.format("exists key: %s, exists: %s", key, redissonUtils.exists(key)));

        redissonUtils.setex(key, value, LOCK_WAIT_TIME_SECOND, SECONDS);
        log.info(String.format("setex key: %s, value: %s, timeToLive: %s, timeUnit: %s", key, value, LOCK_WAIT_TIME_SECOND, SECONDS));
        log.info(String.format("get key: %s, value: %s", key, redissonUtils.get(key)));
        log.info(String.format("redissonString process end, del key: %s, boolean: %s", key, redissonUtils.del(key)));
        log.info("------------>");
    }

    /**
     * 测试redisson-List函数
     */
    @Test
    public void redissonList() {
        final String listKey = "redisson:list";
        final String listKeyTemp = "redisson:list:tmp";
        final List<String> values = Arrays.asList("first", "second", "third", "fourth");
        log.info("<------------ ");
        log.info(String.format("remove listKey: %s, boolean: %s", listKey, redissonUtils.del(listKey)));
        log.info(String.format("remove listKeyTemp: %s, boolean: %s", listKeyTemp, redissonUtils.del(listKeyTemp)));

        redissonUtils.lpush(listKey, values.get(FIRST));
        redissonUtils.lpush(listKey, values.get(ZERO));
        log.info(String.format("lpush listKey: %s, listValues: %s", listKey, redissonUtils.lrange(listKey, ZERO, redissonUtils.llen(listKey))));

        boolean rpush = redissonUtils.rpush(listKey, values.get(THIRD));
        log.info(String.format("rpush listKey: %s, boolean: %s, listValues: %s", listKey, rpush, redissonUtils.lrange(listKey, ZERO, redissonUtils.llen(listKey))));

        Optional<String> rlpop = redissonUtils.rlpop(listKey);
        log.info(String.format("rlpop listKey: %s, value: %s", listKey, rlpop));
        Optional<String> rrpop = redissonUtils.rrpop(listKey);
        log.info(String.format("rrpop listKey: %s, value: %s", listKey, rrpop));

        redissonUtils.lset(listKey, SECOND, values.get(SECOND));
        log.info(String.format("lset listKey: %s, listValues: %s", listKey, redissonUtils.lrange(listKey, ZERO, redissonUtils.llen(listKey))));

        Optional<String> lpop = redissonUtils.lpop(listKey);
        log.info(String.format("lpop listKey: %s, value: %s, listValues: %s", listKey, lpop, redissonUtils.lrange(listKey, ZERO, redissonUtils.llen(listKey))));
        Optional<String> rpop = redissonUtils.rpop(listKey);
        log.info(String.format("lpop listKey: %s, value: %s, listValues: %s", listKey, rpop, redissonUtils.lrange(listKey, ZERO, redissonUtils.llen(listKey))));

        Optional<String> rpoplpush = redissonUtils.rpoplpush(listKey, listKeyTemp);
        log.info(String.format("rpoplpush listKey: %s, listKeyTemp: %s, value: %s, listValues: %s, listKeyTemp: %s", listKey, listKeyTemp, rpoplpush,
                redissonUtils.lrange(listKey, ZERO, redissonUtils.llen(listKey)), redissonUtils.lrange(listKeyTemp, ZERO, redissonUtils.llen(listKeyTemp))));

        redissonUtils.lrem(listKeyTemp, ZERO);
        log.info(String.format("lrem listKey: %s, listValues: %s", listKeyTemp, redissonUtils.lrange(listKey, ZERO, redissonUtils.llen(listKeyTemp))));
        log.info(String.format("redissonList process end, del listKey: %s, boolean: %s", listKey, redissonUtils.del(listKey)));
        log.info(String.format("redissonList process end, del listKeyTemp: %s, boolean: %s", listKeyTemp, redissonUtils.del(listKeyTemp)));
        log.info("------------>");
    }

    /**
     * 测试redisson-Hash函数
     */
    @Test
    public void redissonHash() {
        final String hashKey = "redisson:hash";
        Map<Integer, String> values = new HashMap<Integer, String>(){{
            put(ZERO, String.valueOf(ZERO));
            put(FIRST, String.valueOf(FIRST));
            put(SECOND, String.valueOf(SECOND));
        }};
        log.info("<------------ ");
        log.info(String.format("del hashKey: %s, boolean: %s", hashKey, redissonUtils.del(hashKey)));

        redissonUtils.hmset(hashKey, values, values.size());
        log.info(String.format("hmset hashKey: %s, hashAll: %s", hashKey, redissonUtils.hgetall(hashKey)));
        redissonUtils.hset(hashKey, THIRD, String.valueOf(THIRD));
        log.info(String.format("hset hashKey: %s, hashAll: %s", hashKey, redissonUtils.hgetall(hashKey)));

        Optional<String> hget = redissonUtils.hget(hashKey, THIRD);
        log.info(String.format("hget hashKey: %s, value: %s", hashKey, hget));
        Map<Integer, String> hgetall = redissonUtils.hgetall(hashKey);
        log.info(String.format("hgetall hashKey: %s, values: %s", hashKey, hgetall));
        Map<Integer, String> hmget = redissonUtils.hmget(hashKey, FIRST, SECOND, THIRD);
        log.info(String.format("hmget hashKey: %s, map: %s", hashKey, hmget));

        boolean hexists = redissonUtils.hexists(hashKey, THIRD);
        log.info(String.format("hexists hashKey: %s, boolean: %s", hashKey, hexists));
        Set<Integer> hkeys = redissonUtils.hkeys(hashKey);
        log.info(String.format("hkeys hashKey: %s, keys: %s", hashKey, hkeys));
        Collection<Object> hvals = redissonUtils.hvals(hashKey);
        log.info(String.format("hvals hashKey: %s, values: %s", hashKey, hvals));
        int hlen = redissonUtils.hlen(hashKey);
        log.info(String.format("hlen hashKey: %s, size: %s", hashKey, hlen));

        long hdel = redissonUtils.hdel(hashKey, FIRST, SECOND);
        log.info(String.format("hdel hashKey: %s, hdel: %s, hashAll: %s", hashKey, hdel, redissonUtils.hgetall(hashKey)));
        log.info(String.format("redissonHash process end, del hashKey: %s, boolean: %s", hashKey, redissonUtils.del(hashKey)));
        log.info("------------>");
    }

    /**
     * 测试redisson-SortedSet函数
     */
    @Test
    public void redissonSortedSet() {
        final String sortKey = "redisson:set:sort";
        Map<Integer, String> values = new HashMap<Integer, String>(){{
            put(ZERO, String.valueOf(ZERO));
            put(FIRST, String.valueOf(FIRST));
            put(SECOND, String.valueOf(SECOND));
        }};
        log.info("<------------ ");
        log.info(String.format("del sortKey: %s, boolean: %s", sortKey, redissonUtils.del(sortKey)));

        redissonUtils.zadd(sortKey, ZERO, values.get(ZERO));
        redissonUtils.zadd(sortKey, FIRST, values.get(FIRST));
        redissonUtils.zrange(sortKey, ZERO, END_INDEX).forEach((v) -> log.info(String.format("zadd, value: %s", v)));
        redissonUtils.zadd(sortKey, SECOND, values.get(ZERO));
        redissonUtils.zrangebyscore(sortKey, ZERO, END_INDEX).forEach((v) -> log.info(String.format("zadd overwrite, score: %s, value: %s", v.getScore(), v.getValue())));

        log.info(String.format("zcard sortKey: %s, size: %s", sortKey, redissonUtils.zcard(sortKey)));
        log.info(String.format("zrem sortKey: %s, boolean: %s", sortKey, redissonUtils.zrem(sortKey, values.get(ZERO))));
        redissonUtils.zrangebyscore(sortKey, ZERO, END_INDEX).forEach((v) -> log.info(String.format("zrem, score: %s, value: %s", v.getScore(), v.getValue())));
        log.info(String.format("redissonSortedSet process end, del sortKey: %s, boolean: %s", sortKey, redissonUtils.del(sortKey)));
        log.info("------------>");
    }

    /**
     * 验证redisson分布式锁抢占, 可重入锁, 自动续约功能[默认锁过期时间为30s]
     * 线程1: 抢占锁后睡眠30s -> 重入锁睡眠30s
     * 线程2: 抢占锁后睡眠30s -> 重入锁睡眠30s
     *
     * 结论: 若线程2-70s后抢占成功则验证成功
     */
    public static void main(String[] args) throws IOException {
        final TestRedisson testRedisson = new TestRedisson();
        testRedisson.init();
        testRedisson.redissonLock();
    }

    public void redissonLock() {
        final String lock = "redisson:lock";
        final String threadName1 = "first";
        final String threadName2 = "second";
        new Thread(() -> lockRun(lock), threadName1).start();
        new Thread(() -> lockRun(lock), threadName2).start();
    }

    private void lockRun(String lock) {
        final String ThreadName = Thread.currentThread().getName();
        redissonUtils.lock(lock, Optional.empty(), (v) -> {
            try {
                log.info(String.format("ThreadName: %s, sleep: %s", ThreadName, LOCK_WAIT_TIME_MILLIS));
                Thread.sleep(LOCK_WAIT_TIME_MILLIS);
                // 重入锁
                redissonUtils.lock(lock, Optional.empty(), (e) -> {
                    try {
                        log.info(String.format("ThreadName: %s, sleep: %s", ThreadName, LOCK_WAIT_TIME_MILLIS));
                        Thread.sleep(LOCK_WAIT_TIME_MILLIS);
                    } catch (InterruptedException ignored) {
                    }
                    return Optional.empty();
                });
            } catch (InterruptedException ignored) {
            }
            return Optional.empty();
        });
    }


}
