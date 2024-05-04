package com.example.interviewredis;

import org.junit.jupiter.api.Test;
import org.redisson.api.RHyperLogLog;
import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;

import javax.annotation.Resource;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class InterviewRedisApplicationTests {

    @Autowired
    private RedissonClient redissonClient;
    @Autowired
    private RedisTemplate redisTemplate;

    /**
     * 问题一:假如我要用redis来按天统计我的UV,怎么做?
     */
    @Test
    void contextLoads_uv() {
        RHyperLogLog<Object> hyperLogLog = redissonClient.getHyperLogLog("test");
        for (int i = 0; i < 100000; i++) {
            // 模拟存入的用户ID
            hyperLogLog.add(i);
        }
        System.out.println(hyperLogLog.count());
        /**
         * 插入前,redis内存占用676.23K
         * 模拟插入后:692.14K
         */
    }

    /**
     * 使用set方式统计
     */
    @Test
    void contextLoads_uv_set() {
        RSet<Object> set = redissonClient.getSet("test-set");
        for (int i = 0; i < 100000; i++) {
            set.add(i);
        }
        /**
         * 插入前,redis内存占用692.14K
         * 模拟插入后:8.28M
         */
    }

    /**
     * redis实现分布式ID
     */
    @Test
    void contextLoads_uid() {
        for (int i = 0; i < 1000; i++) {
            Long id = getId("order");
            System.out.println(id);
        }
        /**
         * 1498943587300
         * 1498943587301
         * 1498943587302
         * 1498943587303
         * 1498943587304
         */
    }

    /**
     *
     * @param type 业务类型
     * @return
     */
    public Long getId(String type){
        Long startTime = 1714808352L;
        Long id = redisTemplate.opsForValue().increment(type);
        long time = System.currentTimeMillis()/1000;
        // 时间戳差值左移32位拼接上id自增
        return (time-startTime)<<32 | id;
    }


    /**
     * 缓存击穿实际业务控制,假设100万并发同时打进来
     */
    @Test
    void contextLoads_jichuan() {
        String key = "test";

        // 查询缓存
        List<String> list = (List<String>) redisTemplate.opsForValue().get(key);
        // 缓存中没有,查询数据库
        if(list==null){// 第一重,提高效率,如果有数据,直接返回
            // 加锁控制并发查询DB
            synchronized (InterviewRedisApplicationTests.class){
                // 如果这是第二个并发,那么此时缓存中已经存在了数据
                list = (List<String>) redisTemplate.opsForValue().get(key);
                if(list==null){// 第二重 除了一个进入的线程查询DB外,其他的线程,不需要执行DB操作
                    // 此时是第一个并发,需要查询数据库,并设置缓存
                    // 查询数据库操作
                    list = loadDB();
                    // 设置缓存
                    redisTemplate.opsForValue().set(key,list);
                }
            }
         }
        // 模拟返回数据
        System.out.println(list);
        //return list;
    }

    /**
     * 模拟加载DB
     * @return
     */
    public List<String> loadDB() {
        // 模拟查询数据库1秒
        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        List<String> list = Arrays.asList("aaa", "bbbb");
        return list;
    }

    /**
     * 数据预热方案对比,1.传统设置  2.使用批处理
     */
    @Test
    void contextLoads_PipeLined() {
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            // 每一条指令都是 从客户端 到服务端  服务端返回结果给客户端  存在大量RTT时间
            redisTemplate.opsForValue().set("key"+i,i);
        }
        long endTime = System.currentTimeMillis();
        System.out.println("传统耗时:"+ (endTime - startTime));


        // 使用批处理
        startTime = System.currentTimeMillis();
        redisTemplate.executePipelined(new RedisCallback(){
            @Override
            public Object doInRedis(RedisConnection redisConnection) throws DataAccessException {
                for (int i = 0; i < 10000; i++) {
                    // 减少RTT时间  讲需要批处理的数据打包一次性全部发送给服务端
                    redisConnection.stringCommands().set(("key"+i).getBytes(),String.valueOf(i).getBytes());
                }
                return null;
            }
        });
        endTime = System.currentTimeMillis();
        System.out.println("PipeLined耗时:"+ (endTime - startTime));
        /**
         * 传统耗时:1868
         * PipeLined耗时:128
         */
    }

}
