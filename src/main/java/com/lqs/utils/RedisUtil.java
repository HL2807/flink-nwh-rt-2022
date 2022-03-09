package com.lqs.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Author lqs
 * @Date 2022年03月09日 21:40:18
 * @Version 1.0.0
 * @ClassName RedisUtil
 * @Describe
 */
public class RedisUtil {

    public static JedisPool jedisPool=null;

    public static Jedis getJedis(){
        if (jedisPool==null){
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            //最大的可用连接数量
            jedisPoolConfig.setMaxTotal(100);
            //连接耗尽时是否要等待
            jedisPoolConfig.setBlockWhenExhausted(true);
            //等待的时间
            jedisPoolConfig.setMaxWaitMillis(2000);
            //最大闲置连接数
            jedisPoolConfig.setMaxIdle(5);
            //最小闲置连接数
            jedisPoolConfig.setMinIdle(5);
            //取连接的时候进行一下测试  ping pong
            jedisPoolConfig.setTestOnBorrow(true);

            jedisPool=new JedisPool(jedisPoolConfig,"nwh120",6379,10000);

            System.out.println("开辟连接池");

            return jedisPool.getResource();
        }else {
            //System.out.println(" 连接池:" + jedisPool.getNumActive());
            return jedisPool.getResource();
        }
    }


}

