package org.bigdata.redisson.common.utils;

import static org.bigdata.redisson.common.enums.CommonConstants.END_INDEX;
import static org.bigdata.redisson.common.enums.CommonConstants.FIRST;
import static org.bigdata.redisson.common.enums.CommonConstants.ZERO;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RDeque;
import org.redisson.api.RList;
import org.redisson.api.RLock;
import org.redisson.api.RMap;
import org.redisson.api.RQueue;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.protocol.ScoredEntry;

import org.bigdata.redisson.common.enums.CommonConstants;

/**
 * redisson单例枚举工具类
 * redis原生命令与redisson命令映射关系：https://github.com/redisson/redisson/wiki/11.-Redis-commands-mapping
 * <p>
 * Author: GL
 * Date: 2022-03-20
 */
@Slf4j
public final class RedissonUtils {

    private static volatile RedissonUtils instance;

    private final RedissonClient redisson;

    private RedissonUtils(RedissonClient redisson) {
        this.redisson = redisson;
    }

    public static RedissonUtils getInstance(Optional<RedissonClient> redisson) {
        if (instance == null) {
            synchronized (RedissonUtils.class) {
                if (instance == null) {
                    if (redisson.isPresent()) {
                        instance = new RedissonUtils(redisson.get());
                    } else {
                        throw new NullPointerException("build RedissonUtils failed, RedissonClient is null");
                    }
                }
            }
        }
        return instance;
    }

    /**
     * 返回redisson客户端, 由用户自由实现
     */
    public RedissonClient getRedisson() {
        return redisson;
    }

    //-------------------------------redis-String----------------------------

    /**
     * 获取key值, key不存在返回值为null
     */
    public <T> Optional<T> get(String key) {
        final RBucket<T> bucket = redisson.getBucket(key);
        return Optional.ofNullable(bucket.get());
    }

    /**
     * 设置缓存（注：redisson会自动选择序列化反序列化方式）, 支持key不存在
     */
    public <T> void set(String key, T value) {
        RBucket<T> bucket = redisson.getBucket(key);
        bucket.set(value);
    }

    /**
     * 以string的方式设置缓存&超时时间, 支持key不存在
     */
    public <T> void setex(String key, T value, long timeToLive, TimeUnit timeUnit) {
        RBucket<T> bucket = redisson.getBucket(key);
        bucket.set(value, timeToLive, timeUnit);
    }

    /**
     * 移除缓存, key不存在, 返回false
     */
    public boolean del(String key) {
        return redisson.getBucket(key).delete();
    }

    /**
     * 判断缓存是否存在, key不存在, 返回false
     */
    public boolean exists(String key) {
        return redisson.getBucket(key).isExists();
    }

    //-------------------------------redis-List----------------------------------

    /**
     * 获取列表指定长度list, 如果此队列为空或不存在, 则返回空Optional
     * 注意: [toIndex, toIndex) & fromIndex >= 0 & toIndex <= 列表长度 & fromIndex < toIndex
     */
    public <T> Optional<List<T>> lrange(String key, int fromIndex, int toIndex) {
        final RList<T> list = redisson.getList(key);
        if (list.size() == ZERO) {
            return Optional.empty();
        } else {
            return Optional.of(list.subList(fromIndex, toIndex));
        }
    }

    /**
     * 移出并获取列表的第一个元素, 如果此队列为空或不存在, 则返回空Optional
     */
    public <T> Optional<T> lpop(String key) {
        final RQueue<T> queue = redisson.getQueue(key);
        return Optional.ofNullable(queue.poll());
    }

    /**
     * 移除并获取列表最后一个元素, 如果此队列为空或不存在, 则返回空Optional
     */
    public <T> Optional<T> rpop(String key) {
        final RDeque<T> deque = redisson.getDeque(key);
        return Optional.ofNullable(deque.pollLast());
    }

    /**
     * 读取列表左侧的第一个元素, 如果此队列不存在, 则返回空Optional
     */
    public <T> Optional<T> rlpop(String key) {
        final RList<T> list = redisson.getList(key);
        return Optional.ofNullable(list.get(ZERO));
    }

    /**
     * 读取列表右侧最后一个元素, 如果此队列不存在, 则返回空Optional
     */
    public <T> Optional<T> rrpop(String key) {
        final RList<T> list = redisson.getList(key);
        return Optional.ofNullable(list.get(list.size() - FIRST));
    }

    /**
     * 移除列表的最后一个元素，并将该元素添加到另一个列表并返回
     * 支持source/destination列表不存在, 返回空Optional
     */
    public <T> Optional<T> rpoplpush(String source, String destination) {
        final RDeque<T> deque = redisson.getDeque(source);
        return Optional.ofNullable(deque.pollLastAndOfferFirstTo(destination));
    }

    /**
     * 按指定索引删除对象, 支持列表不存在
     */
    public void lrem(String key, int index) {
        if (exists(key)) {
            redisson.getList(key).fastRemove(index);
        }
    }

    /**
     * 将一个值插入到列表头部, 支持列表不存在
     */
    public <V> void lpush(String key, V elements) {
        redisson.getDeque(key).addFirst(elements);
    }

    /**
     * 在列表尾部中添加一个值, 支持列表不存在
     */
    public <V> boolean rpush(String key, V elements) {
        return redisson.getList(key).add(elements);
    }

    /**
     * 通过索引替换列表元素的值, 支持列表不存在
     */
    public <V> void lset(String key, int index, V element) {
        if (exists(key)) {
            redisson.getList(key).fastSet(index, element);
        }
    }

    /**
     * 获取列表长度, 若列表不存在则返回0
     */
    public int llen(String key) {
        return redisson.getList(key).size();
    }


    //-------------------------------redis-Hash----------------------------------

    /**
     * 删除一个或多个哈希表字段, 若hash不存在返回0
     *
     * @return the number of keys that were removed from the hash, not including specified but non existing keys
     */
    @SafeVarargs
    public final <K> long hdel(String key, K... keys) {
        return redisson.getMap(key).fastRemove(keys);
    }

    /**
     * 查看哈希表 key 中指定的字段是否存在, 若hash不存在返回false
     */
    public <K> boolean hexists(String key, K k) {
        return redisson.getMap(key).containsKey(k);
    }

    /**
     * 获取存储在哈希表中指定字段的值, 若hash不存在, 则返回空Optional
     */
    public <K, V> Optional<V> hget(String key, K k) {
        final RMap<K, V> map = redisson.getMap(key);
        return Optional.ofNullable(map.get(k));
    }

    /**
     * 获取在哈希表中指定 key 的所有字段和值, 若hash不存在, 则返回空map
     */
    public <K, V> Map<K, V> hgetall(String key) {
        final RMap<K, V> map = redisson.getMap(key);
        return map.readAllEntrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    /**
     * 获取所有给定字段的值, 若hash不存在, 则返回空map
     */
    @SafeVarargs
    public final <K, V> Map<K, V> hmget(String key, K... k) {
        final RMap<K, V> map = redisson.getMap(key);
        Set<K> collect = Arrays.stream(k).collect(Collectors.toSet());
        return map.getAll(collect);
    }

    /**
     * 获取所有哈希表中的字段, 若hash不存在, 则返回空set
     */
    public <K> Set<K> hkeys(String key) {
        final RMap<K, Object> map = redisson.getMap(key);
        return map.readAllKeySet();
    }

    /**
     * 获取哈希表中所有值, 若hash不存在, 则返回空collection
     */
    public <V> Collection<V> hvals(String key) {
        final RMap<Object, V> map = redisson.getMap(key);
        return map.readAllValues();
    }

    /**
     * 获取哈希表中字段的数量, 若hash不存在返回0
     */
    public int hlen(String key) {
        return redisson.getMap(key).size();
    }

    /**
     * 将哈希表 key 中的字段 field 的值设为 value, 支持hash不存在
     */
    public <K, V> void hset(String key, K field, V value) {
        final RMap<K, V> map = redisson.getMap(key);
        map.put(field, value);
    }

    /**
     * 同时将多个 field-value (域-值)对设置到哈希表 key 中, 支持hash不存在
     *
     * @param map       mappings to be stored in this map
     * @param batchSize - size of map entries batch
     */
    public <K, V> void hmset(String key, Map<? extends K, ? extends V> map, int batchSize) {
        redisson.getMap(key).putAll(map, batchSize);
    }


    //-------------------------------redis-Set-----------------------------------


    //-------------------------------redis-SortedSet-----------------------------

    /**
     * 向有序集合添加一个, 或者更新已存在成员的分数, 支持key不存在
     * 注意: SortedSet队列不能存在相同的值
     */
    public <V> boolean zadd(String key, double score, V v) {
        final RScoredSortedSet<V> sortedSet = redisson.getScoredSortedSet(key);
        return sortedSet.add(score, v);
    }

    /**
     * 获取有序集合的成员数, 若key不存在返回长度为0
     */
    public int zcard(String key) {
        return redisson.getScoredSortedSet(key).size();
    }

    /**
     * 通过索引区间返回有序集合成指定区间内的成员, 若key不存在返回长度为0的Collection
     * <p>
     * Indexes are zero based.
     * <code>-1</code> means the highest score, <code>-2</code> means the second highest score.
     */
    public <V> Collection<V> zrange(String key, int startIndex, int endIndex) {
        final RScoredSortedSet<V> sortedSet = redisson.getScoredSortedSet(key);
        return sortedSet.valueRange(startIndex, endIndex);
    }

    /**
     * 读取有序列表头元素, 若队列不存在或无元素则返回空Optional
     */
    public <V> Optional<V> zrpop(String key) {
        final RScoredSortedSet<V> sortedSet = redisson.getScoredSortedSet(key);
        Iterator<V> iterator = sortedSet.valueRange(END_INDEX, END_INDEX).iterator();
        return iterator.hasNext() ? Optional.of(iterator.next()) : Optional.empty();
    }

    /**
     * 通过索引区间返回有序集合成指定区间内的成员及权重值, 若key不存在返回长度为0的Collection
     * <p>
     * Indexes are zero based.
     * <code>-1</code> means the highest score, <code>-2</code> means the second highest score.
     */
    public <V> Collection<ScoredEntryEx<V>> zrangebyscore(String key, int startIndex, int endIndex) {
        final RScoredSortedSet<V> sortedSet = redisson.getScoredSortedSet(key);
        return sortedSet.entryRange(startIndex, endIndex).stream().map(ScoredEntryEx::new).collect(Collectors.toList());
    }

    /**
     * 获取有序队列中最高score, 若key不存在返回0.0
     */
    public <V> double zmax(String key) {
        final RScoredSortedSet<V> sortedSet = redisson.getScoredSortedSet(key);
        Iterator<ScoredEntry<V>> iterator = sortedSet.entryRange(END_INDEX, END_INDEX).iterator();
        return iterator.hasNext() ? iterator.next().getScore() : ZERO;
    }

    /**
     * 移除有序集合中的一个成员, 当key不存在时返回false
     */
    public <V> boolean zrem(String key, V v) {
        return redisson.getScoredSortedSet(key).remove(v);
    }

    @Data
    public static final class ScoredEntryEx<V> {
        private final Double score;
        private final V value;

        private ScoredEntryEx(ScoredEntry<V> scoredEntry) {
            super();
            this.score = scoredEntry.getScore();
            this.value = scoredEntry.getValue();
        }
    }

    //-------------------------------redis-lua-----------------------------

    /**
     * 执行lua脚本
     * @param mode - execution mode
     * @param lua - lua script
     * @param keys - keys available through KEYS param in script
     * @param values - values available through VALUES param in script
     */
    public <V> Optional<V> eval(RScript.Mode mode, String lua, List<Object> keys, Object... values) {
        final RScript script = redisson.getScript();
        final V eval = script.eval(mode, lua, RScript.ReturnType.VALUE, keys, values);
        return Objects.nonNull(eval) ? Optional.of(eval) : Optional.empty();
    }

    //-------------------------------redis-分布式锁-----------------------------------

    /**
     * 此分布式锁默认抢占锁后设置的过期时间为30秒, 支持自动续约, 支持可重入
     *
     * @param lockKey 锁
     * @param t       Optional<T>入参
     * @param func    执行函数Lambda表达式
     * @param <T>     入参类型
     * @param <R>     返回值类型
     * @return Optional<R>返回值
     */
    public <T, R> Optional<R> lock(String lockKey, Optional<T> t, Function<Optional<T>, Optional<R>> func) {
        boolean isLock = false;
        Optional<R> result = Optional.empty();
        final RLock lock = redisson.getLock(lockKey);
        final String threadName = Thread.currentThread().getName();
        try {
            while (!isLock) {
                isLock = lock.tryLock(CommonConstants.LOCK_WAIT_TIME_SECOND, TimeUnit.SECONDS);
                if (isLock) {
                    log.debug(String.format(" Lock successfully, execute the function, ThreadName: %s ", threadName));
                    result = func.apply(t);
                } else {
                    log.debug(String.format(" Failed to lock. Try to lock, ThreadName: %s ", threadName));
                }
            }
        } catch (Throwable throwable) {
            log.error(" Throwable locking ", throwable);
        } finally {
            if (lock.isLocked() && lock.isHeldByCurrentThread()) {
                log.debug(String.format(" Release lock , ThreadName: %s ", threadName));
                lock.unlock();
            }
        }
        return result;
    }
}
